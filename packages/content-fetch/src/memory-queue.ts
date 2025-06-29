interface MemoryJob {
  id: string
  data: any
  attempts: number
  maxAttempts: number
  timestamp: number
  priority: number
  backoffDelay: number
}

interface MemoryJobOptions {
  attempts?: number
  priority?: number
  backoff?: {
    type: 'exponential'
    delay: number
  }
}

interface MemoryQueueOptions {
  defaultJobOptions?: {
    backoff?: {
      type: 'exponential'
      delay: number
    }
    removeOnComplete?: {
      age: number
    }
    removeOnFail?: {
      age: number
    }
  }
}

export class MemoryQueue {
  private jobs: MemoryJob[] = []
  private completedJobs: MemoryJob[] = []
  private failedJobs: MemoryJob[] = []
  private isProcessing = false
  private processingFunction?: (job: MemoryJob) => Promise<void>
  private options: MemoryQueueOptions

  constructor(private queueName: string, options: MemoryQueueOptions = {}) {
    this.options = options
    this.startCleanupTimer()
  }

  async addBulk(jobs: Array<{ name: string; data: any; opts?: MemoryJobOptions }>) {
    const memoryJobs = jobs.map((job, index) => ({
      id: `${this.queueName}-${Date.now()}-${index}`,
      data: job.data,
      attempts: 0,
      maxAttempts: job.opts?.attempts || 3,
      timestamp: Date.now(),
      priority: job.opts?.priority || 0,
      backoffDelay: job.opts?.backoff?.delay || 2000,
    }))

    this.jobs.push(...memoryJobs)
    // Sort by priority (lower number = higher priority)
    this.jobs.sort((a, b) => a.priority - b.priority)

    return memoryJobs.map(job => ({ id: job.id }))
  }

  async getJobCounts(types: string[]) {
    const counts: Record<string, number> = {}
    
    for (const type of types) {
      switch (type) {
        case 'active':
          counts.active = this.isProcessing ? 1 : 0
          break
        case 'failed':
          counts.failed = this.failedJobs.length
          break
        case 'completed':
          counts.completed = this.completedJobs.length
          break
        case 'prioritized':
          counts.prioritized = this.jobs.length
          break
        default:
          counts[type] = 0
      }
    }

    return counts
  }

  async getJobs(types: string[], start: number = 0, end: number = -1, asc: boolean = true) {
    if (types.includes('prioritized')) {
      const jobs = [...this.jobs]
      if (!asc) {
        jobs.reverse()
      }
      const sliceEnd = end === -1 ? jobs.length : end + 1
      return jobs.slice(start, sliceEnd)
    }
    return []
  }

  async waitUntilReady() {
    // No-op for memory queue - always ready
    return Promise.resolve()
  }

  private startCleanupTimer() {
    // Clean up old completed and failed jobs every minute
    setInterval(() => {
      const now = Date.now()
      const removeOnCompleteAge = this.options.defaultJobOptions?.removeOnComplete?.age || 3600
      const removeOnFailAge = this.options.defaultJobOptions?.removeOnFail?.age || 24 * 3600

      // Remove old completed jobs
      this.completedJobs = this.completedJobs.filter(
        job => now - job.timestamp < removeOnCompleteAge * 1000
      )

      // Remove old failed jobs
      this.failedJobs = this.failedJobs.filter(
        job => now - job.timestamp < removeOnFailAge * 1000
      )
    }, 60000) // Run every minute
  }
}

export class MemoryWorker {
  private isRunning = false
  private queue: MemoryJob[] = []
  private completedJobs: MemoryJob[] = []
  private failedJobs: MemoryJob[] = []
  private processingInterval?: ReturnType<typeof setInterval>
  private concurrency: number
  private activeJobs = 0
  private limiter: { max: number; duration: number; tokens: number; lastRefill: number }

  constructor(
    private queueName: string,
    private processor: (job: { data: any; attemptsMade: number }) => Promise<void>,
    private options: {
      autorun?: boolean
      concurrency?: number
      limiter?: { max: number; duration: number }
    } = {}
  ) {
    this.concurrency = options.concurrency || 1
    this.limiter = {
      max: options.limiter?.max || 100,
      duration: options.limiter?.duration || 1000,
      tokens: options.limiter?.max || 100,
      lastRefill: Date.now(),
    }

    if (options.autorun) {
      this.start()
    }
  }

  start() {
    if (this.isRunning) return

    this.isRunning = true
    this.processingInterval = setInterval(() => {
      this.processJobs()
    }, 100) // Check for jobs every 100ms
  }

  async close() {
    this.isRunning = false
    if (this.processingInterval) {
      clearInterval(this.processingInterval)
      this.processingInterval = undefined
    }

    // Wait for active jobs to complete
    while (this.activeJobs > 0) {
      await new Promise(resolve => setTimeout(resolve, 100))
    }
  }

  on(event: string, handler: (error: Error) => void) {
    if (event === 'error') {
      // Store error handler for later use
      this.errorHandler = handler
    }
  }

  private errorHandler?: (error: Error) => void

  addJob(job: MemoryJob) {
    this.queue.push(job)
    // Sort by priority (lower number = higher priority)
    this.queue.sort((a, b) => a.priority - b.priority)
  }

  private refillTokens() {
    const now = Date.now()
    const timePassed = now - this.limiter.lastRefill
    const tokensToAdd = Math.floor((timePassed / this.limiter.duration) * this.limiter.max)
    
    if (tokensToAdd > 0) {
      this.limiter.tokens = Math.min(this.limiter.max, this.limiter.tokens + tokensToAdd)
      this.limiter.lastRefill = now
    }
  }

  private async processJobs() {
    if (!this.isRunning || this.activeJobs >= this.concurrency) {
      return
    }

    this.refillTokens()
    if (this.limiter.tokens <= 0) {
      return
    }

    const job = this.queue.shift()
    if (!job) {
      return
    }

    this.limiter.tokens--
    this.activeJobs++

    try {
      await this.processor({
        data: job.data,
        attemptsMade: job.attempts,
      })

      // Job completed successfully
      this.completedJobs.push({
        ...job,
        timestamp: Date.now(),
      })
    } catch (error) {
      job.attempts++

      if (job.attempts >= job.maxAttempts) {
        // Job failed permanently
        this.failedJobs.push({
          ...job,
          timestamp: Date.now(),
        })
      } else {
        // Retry job with exponential backoff
        const delay = job.backoffDelay * Math.pow(2, job.attempts - 1)
        setTimeout(() => {
          if (this.isRunning) {
            this.addJob(job)
          }
        }, delay)
      }

      if (this.errorHandler) {
        this.errorHandler(error as Error)
      }
    } finally {
      this.activeJobs--
    }
  }

  getJobCounts() {
    return {
      active: this.activeJobs,
      failed: this.failedJobs.length,
      completed: this.completedJobs.length,
      prioritized: this.queue.length,
    }
  }

  getJobs(types: string[], start: number = 0, end: number = -1, asc: boolean = true) {
    if (types.includes('prioritized')) {
      const jobs = [...this.queue]
      if (!asc) {
        jobs.reverse()
      }
      const sliceEnd = end === -1 ? jobs.length : end + 1
      return jobs.slice(start, sliceEnd)
    }
    return []
  }
}

// Global registry to share queues and workers between different parts of the application
class MemoryQueueRegistry {
  private queues = new Map<string, MemoryQueue>()
  private workers = new Map<string, MemoryWorker>()

  getQueue(queueName: string, options?: MemoryQueueOptions): MemoryQueue {
    if (!this.queues.has(queueName)) {
      this.queues.set(queueName, new MemoryQueue(queueName, options))
    }
    return this.queues.get(queueName)!
  }

  createWorker(
    queueName: string,
    processor: (job: { data: any; attemptsMade: number }) => Promise<void>,
    options?: {
      autorun?: boolean
      concurrency?: number
      limiter?: { max: number; duration: number }
    }
  ): MemoryWorker {
    if (this.workers.has(queueName)) {
      throw new Error(`Worker for queue ${queueName} already exists`)
    }

    const worker = new MemoryWorker(queueName, processor, options)
    this.workers.set(queueName, worker)

    // Connect worker to queue for job processing
    const queue = this.getQueue(queueName)
    this.connectWorkerToQueue(worker, queue)

    return worker
  }

  private connectWorkerToQueue(worker: MemoryWorker, queue: MemoryQueue) {
    // Override queue's addBulk to also add jobs to worker
    const originalAddBulk = queue.addBulk.bind(queue)
    queue.addBulk = async (jobs) => {
      const result = await originalAddBulk(jobs)
      
      // Add jobs to worker for processing
      const memoryJobs = jobs.map((job, index) => ({
        id: `${queue['queueName']}-${Date.now()}-${index}`,
        data: job.data,
        attempts: 0,
        maxAttempts: job.opts?.attempts || 3,
        timestamp: Date.now(),
        priority: job.opts?.priority || 0,
        backoffDelay: job.opts?.backoff?.delay || 2000,
      }))

      memoryJobs.forEach(job => worker.addJob(job))
      
      return result
    }
  }

  async shutdown() {
    // Close all workers
    await Promise.all(
      Array.from(this.workers.values()).map(worker => worker.close())
    )
    
    this.workers.clear()
    this.queues.clear()
  }
}

export const memoryQueueRegistry = new MemoryQueueRegistry()