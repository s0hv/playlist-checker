import express from 'express';
import expressWinston from 'express-winston';
import http from 'http';

import indexRouter from './routes/index.js';

import { rateLimiter } from './utils/ratelimiter.js';
import { getTransports } from './utils/logging.js';


const app = express();

const loggerFormat = process.env.EXPRESS_LOG_FORMAT || "{{res.statusCode}} {{req.method}} {{res.responseTime}}ms {{req.ip}} {{req.url}}"
app.use(expressWinston.logger({
  transports: getTransports(),
  colorize: true,
  msg: loggerFormat,
  meta: true
}));

app.use(rateLimiter);
app.use('/', indexRouter);
app.use('*', (req, res) => res.status(404).end())

app.use(expressWinston.errorLogger({
  transports: getTransports(),
  msg: loggerFormat,
}))

const server = http.createServer(app);

/**
 * Listen on provided port, on all network interfaces.
 */
server.listen(process.env.PORT || '3000', () => console.log('Live'));
