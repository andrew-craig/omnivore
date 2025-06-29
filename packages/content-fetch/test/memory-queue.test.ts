import 'mocha'
import { expect } from 'chai'
import { MemoryQueue, MemoryWorker, memoryQueueRegistry } from '../src/memory-queue'

describe('MemoryQueue', () => {
  it('should create a queue and add jobs', async () => {
    const queue = new MemoryQueue('test-queue')
    
    const jobs = await queue.addBulk([
      { name: 'test-job', data: { url: 'https://example.com' } },
      { name: 'test-job', data: { url: 'https://example2.com' } }
    ])
    
    expect(jobs).to.have.length(2)
    expect(jobs[0]).to.have.property('id')
    expect(jobs[1]).to.have.property('id')
  })

  it('should return correct job counts', async () => {
    const queue = new MemoryQueue('test-queue-2')
    
    await queue.addBulk([
      { name: 'test-job', data: { url: 'https://example.com' } }
    ])
    
    const counts = await queue.getJobCounts(['prioritized', 'active', 'failed', 'completed'])
    
    expect(counts.prioritized).to.equal(1)
    expect(counts.active).to.equal(0)
    expect(counts.failed).to.equal(0)
    expect(counts.completed).to.equal(0)
  })

  it('should handle priority ordering', async () => {
    const queue = new MemoryQueue('test-queue-3')
    
    await queue.addBulk([
      { name: 'low-priority', data: { url: 'https://low.com' }, opts: { priority: 10 } },
      { name: 'high-priority', data: { url: 'https://high.com' }, opts: { priority: 1 } },
      { name: 'medium-priority', data: { url: 'https://medium.com' }, opts: { priority: 5 } }
    ])
    
    const jobs = await queue.getJobs(['prioritized'], 0, 2)
    expect(jobs).to.have.length(3)
    expect(jobs[0].priority).to.equal(1) // highest priority first
    expect(jobs[1].priority).to.equal(5)
    expect(jobs[2].priority).to.equal(10)
  })
})

describe('MemoryWorker', () => {
  it('should process jobs successfully', async () => {
    const processedJobs: any[] = []
    
    const worker = new MemoryWorker(
      'test-worker-queue',
      async (job) => {
        processedJobs.push(job.data)
      },
      { autorun: false }
    )
    
    // Add a job directly to the worker
    worker.addJob({
      id: 'test-1',
      data: { url: 'https://test.com' },
      attempts: 0,
      maxAttempts: 3,
      timestamp: Date.now(),
      priority: 1,
      backoffDelay: 1000
    })
    
    // Start processing
    worker.start()
    
    // Wait a bit for processing
    await new Promise(resolve => setTimeout(resolve, 200))
    
    expect(processedJobs).to.have.length(1)
    expect(processedJobs[0].url).to.equal('https://test.com')
    
    await worker.close()
  })

  it('should handle job failures with retry', async () => {
    let attemptCount = 0
    const errors: Error[] = []
    
    const worker = new MemoryWorker(
      'test-worker-queue-2',
      async (job) => {
        attemptCount++
        if (attemptCount < 3) {
          throw new Error(`Attempt ${attemptCount} failed`)
        }
        // Succeed on third attempt
      },
      { autorun: false }
    )
    
    worker.on('error', (err) => {
      errors.push(err)
    })
    
    // Add a job that will fail initially
    worker.addJob({
      id: 'test-retry',
      data: { url: 'https://retry-test.com' },
      attempts: 0,
      maxAttempts: 3,
      timestamp: Date.now(),
      priority: 1,
      backoffDelay: 50 // Short delay for testing
    })
    
    worker.start()
    
    // Wait for retries to complete
    await new Promise(resolve => setTimeout(resolve, 500))
    
    expect(attemptCount).to.equal(3)
    expect(errors).to.have.length(2) // Two failures before success
    
    await worker.close()
  })
})

describe('MemoryQueueRegistry', () => {
  it('should create and manage queues', () => {
    const queue1 = memoryQueueRegistry.getQueue('registry-test-1')
    const queue2 = memoryQueueRegistry.getQueue('registry-test-1') // Same name
    const queue3 = memoryQueueRegistry.getQueue('registry-test-2') // Different name
    
    expect(queue1).to.equal(queue2) // Should return same instance
    expect(queue1).to.not.equal(queue3) // Should return different instance
  })

  it('should create workers and connect them to queues', async () => {
    const processedJobs: any[] = []
    
    const worker = memoryQueueRegistry.createWorker(
      'registry-worker-test',
      async (job) => {
        processedJobs.push(job.data)
      },
      { autorun: true }
    )
    
    const queue = memoryQueueRegistry.getQueue('registry-worker-test')
    
    // Add jobs to the queue
    await queue.addBulk([
      { name: 'test-job', data: { url: 'https://registry-test.com' } }
    ])
    
    // Wait for processing
    await new Promise(resolve => setTimeout(resolve, 200))
    
    expect(processedJobs).to.have.length(1)
    expect(processedJobs[0].url).to.equal('https://registry-test.com')
    
    await worker.close()
  })
})