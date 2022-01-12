import defaultConfig from '../config.default.js';

/**
 * @type {{
 *      allowedExtensions: Set<string>,
 *      mimetypes: {},
 *      throttleRate: Number
 *    }}
 */
let config = defaultConfig;

try {
  const customConfig = require('config.js');
  config = {
    ...config,
    ...customConfig
  };

} catch {}

config.allowedExtensions = new Set(config.allowedExtensions)

export default config;
