/**
 * A basic mock implementation of Redis for testing
 * This allows testing the RedisJobStorage without an actual Redis server
 */
export class RedisMock {
  private storage: Map<string, any> = new Map();
  private sets: Map<string, Set<string>> = new Map();

  // Basic key-value operations
  async get(key: string): Promise<any> {
    return this.storage.get(key) || null;
  }

  async set(key: string, value: any, ...args: any[]): Promise<string | null> {
    // Check if NX (not exists) flag is provided
    if (args.includes('NX') && this.storage.has(key)) {
      return null;
    }

    // Check for EX (expiry) flag
    const exIndex = args.indexOf('EX');
    if (exIndex !== -1 && args.length > exIndex + 1) {
      // In a real implementation, this would set an expiry
      // For the mock, we just pretend it's set
    }

    this.storage.set(key, value);
    return 'OK';
  }

  async del(key: string): Promise<number> {
    const deleted = this.storage.delete(key);
    return deleted ? 1 : 0;
  }

  // Set operations
  async sadd(key: string, ...members: string[]): Promise<number> {
    if (!this.sets.has(key)) {
      this.sets.set(key, new Set());
    }
    
    const set = this.sets.get(key)!;
    let addedCount = 0;
    
    for (const member of members) {
      if (!set.has(member)) {
        set.add(member);
        addedCount++;
      }
    }
    
    return addedCount;
  }

  async srem(key: string, ...members: string[]): Promise<number> {
    if (!this.sets.has(key)) {
      return 0;
    }
    
    const set = this.sets.get(key)!;
    let removedCount = 0;
    
    for (const member of members) {
      if (set.has(member)) {
        set.delete(member);
        removedCount++;
      }
    }
    
    return removedCount;
  }

  async smembers(key: string): Promise<string[]> {
    if (!this.sets.has(key)) {
      return [];
    }
    
    return Array.from(this.sets.get(key)!);
  }

  // Pipeline support
  pipeline(): Pipeline {
    return new Pipeline(this);
  }
}

/**
 * Mock implementation of Redis pipeline
 */
class Pipeline {
  private commands: Array<{ command: string; args: any[]; callback: Function }> = [];
  private redis: RedisMock;

  constructor(redis: RedisMock) {
    this.redis = redis;
  }

  // Support for common commands
  set(...args: any[]): Pipeline {
    this.commands.push({
      command: 'set',
      args,
      callback: () => this.redis.set(args[0], args[1])
    });
    return this;
  }

  get(...args: any[]): Pipeline {
    this.commands.push({
      command: 'get',
      args,
      callback: () => this.redis.get(args[0])
    });
    return this;
  }

  sadd(...args: any[]): Pipeline {
    this.commands.push({
      command: 'sadd',
      args,
      callback: () => {
        const key = args[0];
        const members = args.slice(1);
        return this.redis.sadd(key, ...members);
      }
    });
    return this;
  }

  srem(...args: any[]): Pipeline {
    this.commands.push({
      command: 'srem',
      args,
      callback: () => {
        const key = args[0];
        const members = args.slice(1);
        return this.redis.srem(key, ...members);
      }
    });
    return this;
  }

  // Execute the pipeline
  async exec(): Promise<Array<[Error | null, any]>> {
    const results: Array<[Error | null, any]> = [];
    
    for (const { callback } of this.commands) {
      try {
        const result = await callback();
        results.push([null, result]);
      } catch (error) {
        results.push([error as Error, null]);
      }
    }
    
    return results;
  }
} 