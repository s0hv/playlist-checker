import express from 'express';
import logger from 'morgan';
import http from 'http';

import indexRouter from './routes/index.js';

import { rateLimiter } from './utils/ratelimiter.js';


const app = express();

app.use(logger('dev'));
app.use(rateLimiter);

app.use('/', indexRouter);

const server = http.createServer(app);

/**
 * Listen on provided port, on all network interfaces.
 */
server.listen(process.env.PORT || '3000', () => console.log('Live'));
