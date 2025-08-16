import express from 'express';
import pinoHttp from 'pino-http';
import http from 'http';

import indexRouter from './routes/index.js';

import { ddosLimiter, rateLimiter } from './utils/ratelimiter.js';
import { logger } from './utils/logging.js';


const app = express();

if (/y|yes|true|1/i.test(process.env.PROXIED)) {
  app.set('trust proxy', 'loopback');
}

app.disable('x-powered-by');


app.use(pinoHttp({
  logger,
   customProps: function (req) {
    return {
      ip: req.ip,
      urlDecoded: decodeURIComponent(req.url === '/' ? req.originalUrl : req.url)
    }
  }
}));

app.use(rateLimiter);
app.use('/', indexRouter);
app.use('*catchAll', async (req, res) => {
  res.status(404).end()
  // When bogus URLs are requested, consume many tokens from the ddos limiter
  await ddosLimiter.consume(req.ip, 20)
    .catch()
})

// Error handler so pino can log errors
app.use((err, req, res, next) => {
  // set error for pino-http to log
  res.err = err;
  // let express default error handler handle the error (optional)
  next();
});

const server = http.createServer(app);

/**
 * Listen on provided port, on all network interfaces.
 */
server.listen(process.env.PORT || '3000', () => console.log('Live'));
