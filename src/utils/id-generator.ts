// src/utils/id-generator.ts

/**
 * Generates a random string of specified length with given character set
 * Compatible with all JavaScript runtimes (Node.js, Deno, Bun, browsers)
 * 
 * @param length The length of the ID to generate
 * @param chars Character set to use (defaults to alphanumeric)
 * @returns A random ID string
 */
export function generateId(
    length: number = 21,
    chars: string = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'
  ): string {
    let id = '';
    try {
      // Try to use crypto if available
      const crypto = globalThis.crypto;
      if (crypto?.getRandomValues) {
        const bytes = new Uint8Array(length);
        crypto.getRandomValues(bytes);
        
        for (let i = 0; i < length; i++) {
          id += chars.charAt(bytes[i] % chars.length);
        }
        return id;
      }
    } catch (e) {
      // Fall back to Math.random if crypto is not available or fails
    }
    // Fallback to Math.random (less secure but works everywhere)
    for (let i = 0; i < length; i++) {
      id += chars.charAt(Math.floor(Math.random() * chars.length));
    }
    return id;
  }