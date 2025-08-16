import { RateLimiterMemory, RateLimiterRedis } from 'rate-limiter-flexible';
import Redis from 'ioredis';
import config from './config.js';
import { Transform } from 'stream';
import { day, gigabyte, minute } from './units.js';
import { logger } from './logging.js';

const isDev = process.env.NODE_ENV !== 'production';

const redis = new Redis(process.env.REDIS_URL, {
  showFriendlyErrorStack: isDev,
  retryStrategy: (times) => {
    if (times > 3) {
      return new Error('Redis offline');
    }
    return 200;
  }
});

redis.on('error', err => {
  logger.error('Redis error', err);
});

// In case redis breaks allow some request to go through
const emergencyMemoryLimiter = new RateLimiterMemory({
  points: 5_000,
  duration: day
});

const redisLimiter = new RateLimiterRedis({
  storeClient: redis,
  points: 50_000,
  duration: day,
  insuranceLimiter: emergencyMemoryLimiter,
  keyPrefix: 'rlflx',
});

export const ddosLimiter = new RateLimiterMemory({
  // Block requests when a burst of 100 in a minute happens
  points: 100,
  duration: minute
});

export const downloadThrottler = new RateLimiterMemory({
  points: config.throttleRate * gigabyte,
  duration: day
});

const dlConsume = (points) => downloadThrottler.consume(key, points);

export class ThrottleDownloads extends Transform {
  constructor(consume, opts) {
    super(opts);
    this.readable = true;
    this.writable = true;

    this.consume = consume || dlConsume;
  }

  _transform(chunk, encoding, callback) {
    this.consume(chunk.length)
      .then(() => {
        this.push(chunk, encoding);
        callback();
      })
      .catch(err => this.emit('error', err));
  }
}

const key = 'global';
export const rateLimiter = (req, res, next) => {
  const ip = req.ip;
  ddosLimiter.consume(ip, 1)
    .then(() => redisLimiter.consume(key, 1))
    .then(() => next())
    .catch(() => {
      res.status(429).end();
    });
}
