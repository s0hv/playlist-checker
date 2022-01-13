import express from 'express';
import expressWinston from 'express-winston';
import http from 'http';

import indexRouter from './routes/index.js';

import { rateLimiter } from './utils/ratelimiter.js';
import { getTransports } from './utils/logging.js';


const app = express();

app.use(expressWinston.logger({
  transports: getTransports(),
  colorize: true,
  msg: "{{res.statusCode}} {{req.method}} {{res.responseTime}}ms {{req.url}}",
  meta: false
}));

app.use(rateLimiter);
app.use('/', indexRouter);

app.use(expressWinston.errorLogger({
  transports: getTransports(),
  msg: "{{res.statusCode}} {{req.method}} {{res.responseTime}}ms {{req.url}}",
}))

const server = http.createServer(app);

/**
 * Listen on provided port, on all network interfaces.
 */
server.listen(process.env.PORT || '3000', () => console.log('Live'));
