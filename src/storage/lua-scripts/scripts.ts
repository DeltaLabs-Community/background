export const atomicAcquireScript: string = `
    -- KEYS[1] = priority queue key prefix
    -- KEYS[2] = job key prefix
    -- KEYS[3] = status key prefix
    -- KEYS[4] = scheduled jobs key
    -- ARGV[1] = current timestamp
    -- ARGV[2] = stale timeout
    -- ARGV[3] = handler names count
    -- ARGV[4+] = handler names

    local priorityPrefix = KEYS[1]
    local jobPrefix = KEYS[2]
    local statusPrefix = KEYS[3]
    local scheduledKey = KEYS[4]
    local now = tonumber(ARGV[1])
    local staleTimeout = tonumber(ARGV[2])
    local handlerCount = tonumber(ARGV[3]) or 0
    
    -- Build handler names set
    local handlerSet = {}
    if handlerCount > 0 then
        for i = 1, handlerCount do
            local handlerName = ARGV[3 + i]
            handlerSet[handlerName] = true
            redis.log(redis.LOG_WARNING, "Handler registered: " .. handlerName)
        end
    end

    -- Helper function
    local function matchesHandler(jobName)
        if handlerCount == 0 then
            redis.log(redis.LOG_WARNING, "No handler filter, accepting job: " .. (jobName or "nil"))
            return true
        end
        local matches = handlerSet[jobName] ~= nil
        redis.log(redis.LOG_WARNING, "Job '" .. (jobName or "nil") .. "' matches handler: " .. tostring(matches))
        return matches
    end

    -- 1. Move scheduled jobs
    local dueJobs = redis.call('ZRANGEBYSCORE', scheduledKey, 0, now)
    redis.log(redis.LOG_WARNING, "Due scheduled jobs: " .. #dueJobs)
    
    for i, jobId in ipairs(dueJobs) do
        redis.call('ZREM', scheduledKey, jobId)
        local jobKey = jobPrefix .. jobId
        if redis.call('EXISTS', jobKey) == 1 then
            local status = redis.call('HGET', jobKey, 'status')
            local priority = tonumber(redis.call('HGET', jobKey, 'priority')) or 3
            if status == 'pending' then
                priority = math.max(1, math.min(10, priority))
                redis.call('LPUSH', priorityPrefix .. priority, jobId)
                redis.log(redis.LOG_WARNING, "Moved scheduled job " .. jobId .. " to priority " .. priority)
            end
        end
    end

    -- 2. Try to acquire from priority queues
    for priority = 1, 10 do
        local queueKey = priorityPrefix .. priority
        local queueLength = redis.call('LLEN', queueKey)
        
        if queueLength > 0 then
            redis.log(redis.LOG_WARNING, "Priority " .. priority .. " queue has " .. queueLength .. " jobs")
        end
        
        local maxAttempts = math.min(100, queueLength + 1)
        local attempts = 0
        
        while attempts < maxAttempts do
            attempts = attempts + 1
            local jobId = redis.call('RPOP', queueKey)
            
            if not jobId then
                break
            end
            
            redis.log(redis.LOG_WARNING, "Checking job: " .. jobId)
            local jobKey = jobPrefix .. jobId
            
            if redis.call('EXISTS', jobKey) == 1 then
                local currentStatus = redis.call('HGET', jobKey, 'status')
                
                if currentStatus == 'pending' then
                    local jobName = redis.call('HGET', jobKey, 'name')
                    
                    if matchesHandler(jobName) then
                        -- Acquire the job
                        local oldStatusKey = statusPrefix .. currentStatus
                        local newStatusKey = statusPrefix .. 'processing'
                        
                        -- Use HSET with multiple field-value pairs
                        redis.call('HSET', jobKey, 'status', 'processing', 'startedAt', tostring(now))
                        redis.call('SREM', oldStatusKey, jobId)
                        redis.call('SADD', newStatusKey, jobId)
                        
                        redis.log(redis.LOG_WARNING, "ACQUIRED JOB: " .. jobId)
                        return jobId
                    else
                        -- Put back at end
                        redis.call('LPUSH', queueKey, jobId)
                    end
                else
                end
            else
            end
        end
    end

    -- 3. Check stale jobs
    local staleThreshold = now - staleTimeout
    local processingKey = statusPrefix .. 'processing'
    local processingJobs = redis.call('SMEMBERS', processingKey)
    redis.log(redis.LOG_WARNING, "Checking " .. #processingJobs .. " processing jobs for staleness")

    for i, jobId in ipairs(processingJobs) do
        local jobKey = jobPrefix .. jobId
        if redis.call('EXISTS', jobKey) == 1 then
            local startedAtStr = redis.call('HGET', jobKey, 'startedAt')
            local jobName = redis.call('HGET', jobKey, 'name')
                        
            if startedAtStr and matchesHandler(jobName) then
                local startedAt = tonumber(startedAtStr)
                if startedAt and startedAt < staleThreshold then
                    redis.call('HSET', jobKey, 'startedAt', tostring(now))
                    return jobId
                end
            end
        end
    end
    return nil
`;

//----------------------------------------------------------------------------

export const failJobScript : string = `
      -- Arguments:
      -- KEYS[1]: Job key
      -- KEYS[2]: Status set key prefix 
      
      -- ARGV[1]: Job ID
      -- ARGV[2]: Error message
      -- ARGV[3]: Completion timestamp
      
      local jobKey = KEYS[1]
      local statusKeyPrefix = KEYS[2]
      
      local jobId = ARGV[1]
      local errorMsg = ARGV[2]
      local completedAt = ARGV[3]
      
      -- Check if job exists
      if redis.call('EXISTS', jobKey) == 0 then
        return {err = "Job not found: " .. jobId}
      end
      
      -- Get current status
      local currentStatus = redis.call('HGET', jobKey, 'status')
      
      -- Update job data
      redis.call('HSET', jobKey, 
        'status', 'failed',
        'error', errorMsg,
        'completedAt', completedAt
      )
      
      -- Update status sets
      if currentStatus then
        redis.call('SREM', statusKeyPrefix .. currentStatus, jobId)
      end
      redis.call('SADD', statusKeyPrefix .. 'failed', jobId)
      
      return 1
`;

// ---------------------------------------------------------------

export const completeJobScript : string = `
      -- Arguments:
      -- KEYS[1]: Job key
      -- KEYS[2]: Status set key prefix 
      
      -- ARGV[1]: Job ID
      -- ARGV[2]: Result (JSON string)
      -- ARGV[3]: Completion timestamp
      
      local jobKey = KEYS[1]
      local statusKeyPrefix = KEYS[2]
      
      local jobId = ARGV[1]
      local result = ARGV[2]
      local completedAt = ARGV[3]
      
      -- Check if job exists
      if redis.call('EXISTS', jobKey) == 0 then
        return {err = "Job not found: " .. jobId}
      end
      
      -- Get current status
      local currentStatus = redis.call('HGET', jobKey, 'status')
      
      -- Update job data
      redis.call('HSET', jobKey, 
        'status', 'completed',
        'result', result,
        'completedAt', completedAt
      )
      
      -- Update status sets
      if currentStatus then
        redis.call('SREM', statusKeyPrefix .. currentStatus, jobId)
      end
      redis.call('SADD', statusKeyPrefix .. 'completed', jobId)
      
      return 1
`;

// -----------------------------------------------------------

export const moveScheduledJobsScript : string = `
      -- Arguments:
      -- KEYS[1]: Scheduled jobs sorted set
      -- KEYS[2]: Job key prefix
      -- KEYS[3]: Priority queue key prefix
      
      -- ARGV[1]: Current timestamp
      
      local scheduledKey = KEYS[1]
      local jobKeyPrefix = KEYS[2]
      local priorityKeyPrefix = KEYS[3]
      local now = tonumber(ARGV[1])
      
      -- Get all jobs scheduled before now
      local dueJobs = redis.call('ZRANGEBYSCORE', scheduledKey, 0, now)
      if #dueJobs == 0 then
        return 0  -- No jobs due yet
      end
      
      local movedCount = 0
      
      -- Process each due job
      for i, jobId in ipairs(dueJobs) do
        -- Remove job from scheduled set
        redis.call('ZREM', scheduledKey, jobId)
        
        -- Get job details to find priority
        local jobKey = jobKeyPrefix .. jobId
        
        -- Check if job still exists
        if redis.call('EXISTS', jobKey) == 1 then
          -- Get current status and priority
          local status = redis.call('HGET', jobKey, 'status')
          local priority = tonumber(redis.call('HGET', jobKey, 'priority')) or 3
          
          -- Only move to queue if job is still pending
          if status == 'pending' then
            -- Make sure priority is within bounds
            priority = math.max(1, math.min(10, priority))
            
            -- Add to appropriate priority queue
            redis.call('LPUSH', priorityKeyPrefix .. priority, jobId)
            movedCount = movedCount + 1
          end
        end
      end
      
      return movedCount
`;
// ---------------------------------------------------------

export const updateJobScript : string = `
      -- Arguments:
      -- KEYS[1]: Job key (hash)
      -- KEYS[2]: New status set key 
      -- KEYS[3]: Scheduled set key
      -- KEYS[4]: Priority queue key prefix
      -- KEYS[5]: Status set key prefix
      
      -- ARGV[1]: Job ID
      -- ARGV[2]: Current status (for old status set)
      -- ARGV[3]: New status
      -- ARGV[4]: Priority (default 3)
      -- ARGV[5]: Is scheduled (1 or 0)
      -- ARGV[6]: Scheduled time (timestamp)
      -- ARGV[7+]: Key-value pairs for job data (flattened)
      
      local jobKey = KEYS[1]
      local newStatusKey = KEYS[2]
      local scheduledKey = KEYS[3]
      local priorityKeyPrefix = KEYS[4]
      local statusKeyPrefix = KEYS[5]
      
      local jobId = ARGV[1]
      local oldStatus = ARGV[2]
      local newStatus = ARGV[3]
      local priority = tonumber(ARGV[4]) or 3
      local isScheduled = ARGV[5] == "1"
      local scheduledTime = tonumber(ARGV[6]) or 0
      
      -- Check if job exists
      if redis.call('EXISTS', jobKey) == 0 then
        return {err = "Job not found: " .. jobId}
      end
      
      -- Update job data
      for i = 7, #ARGV, 2 do
        redis.call('HSET', jobKey, ARGV[i], ARGV[i+1])
      end
      
      -- Update status sets if status changed
      if oldStatus ~= newStatus then
        local oldStatusKey = statusKeyPrefix .. oldStatus
        redis.call('SREM', oldStatusKey, jobId)
        redis.call('SADD', newStatusKey, jobId)
      end
      
      -- Handle queue placement based on status
      if newStatus == 'pending' then
        if isScheduled then
          -- Remove from all priority queues
          for p = 1, 10 do
            redis.call('LREM', priorityKeyPrefix .. p, 0, jobId)
          end
          -- Add to scheduled set
          redis.call('ZADD', scheduledKey, scheduledTime, jobId)
        else
          -- Remove from scheduled set
          redis.call('ZREM', scheduledKey, jobId)
          
          -- Remove from all other priority queues
          for p = 1, 10 do
            if p ~= priority then
              redis.call('LREM', priorityKeyPrefix .. p, 0, jobId)
            end
          end
          
          -- Add to correct priority queue
          redis.call('LPUSH', priorityKeyPrefix .. priority, jobId)
        end
      else
        -- If not pending, remove from all queues
        redis.call('ZREM', scheduledKey, jobId)
        for p = 1, 10 do
          redis.call('LREM', priorityKeyPrefix .. p, 0, jobId)
        end
      end
      
      return 1
`;

//--------------------------------------------------------------
export const saveJobScript: string = `
      -- Arguments:
      -- KEYS[1]: Job key (hash)
      -- KEYS[2]: Status set key
      -- KEYS[3]: Scheduled set key
      -- KEYS[4]: Priority queue key prefix
      
      -- ARGV[1]: Job ID
      -- ARGV[2]: Job status
      -- ARGV[3]: Priority (default 3)
      -- ARGV[4]: Is scheduled (1 or 0)
      -- ARGV[5]: Scheduled time (timestamp)
      -- ARGV[6+]: Key-value pairs for job data (flattened)
      
      local jobKey = KEYS[1]
      local statusKey = KEYS[2]
      local scheduledKey = KEYS[3]
      local priorityKeyPrefix = KEYS[4]
      
      local jobId = ARGV[1]
      local status = ARGV[2]
      local priority = tonumber(ARGV[3]) or 3
      local isScheduled = ARGV[4] == "1"
      local scheduledTime = tonumber(ARGV[5]) or 0
      
      -- DEBUG: Log what we're receiving
      redis.log(redis.LOG_WARNING, "Saving job " .. jobId .. " with status " .. status)
      
      -- Store ALL job hash fields from ARGV[6] onwards
      -- This is CRITICAL - we must save all the fields including 'name'
      if #ARGV >= 6 then
        local fields = {}
        for i = 6, #ARGV, 2 do
          if ARGV[i] and ARGV[i+1] then
            table.insert(fields, ARGV[i])
            table.insert(fields, ARGV[i+1])
            -- DEBUG: Log each field being saved
            redis.log(redis.LOG_WARNING, "Setting field: " .. ARGV[i] .. " = " .. ARGV[i+1])
          end
        end
        
        if #fields > 0 then
          redis.call('HMSET', jobKey, unpack(fields))
        end
      end
      
      -- Add to status set
      redis.call('SADD', statusKey, jobId)
      
      -- Add to appropriate queue based on status and scheduling
      if status == 'pending' then
        if isScheduled then
          redis.call('ZADD', scheduledKey, scheduledTime, jobId)
          redis.log(redis.LOG_WARNING, "Added job " .. jobId .. " to scheduled set")
        else
          -- Ensure priority is within bounds
          priority = math.max(1, math.min(10, priority))
          local priorityQueueKey = priorityKeyPrefix .. priority
          redis.call('LPUSH', priorityQueueKey, jobId)
          redis.log(redis.LOG_WARNING, "Added job " .. jobId .. " to priority queue " .. priority)
        end
      end
      
      -- Verify what was saved (DEBUG)
      local savedName = redis.call('HGET', jobKey, 'name')
      redis.log(redis.LOG_WARNING, "Verified saved name: " .. (savedName or "NIL"))
      
      return 1
`;