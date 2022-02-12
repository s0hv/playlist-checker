import defaultConfig from '../config.default.js';

/**
 * @type {{
 *      allowedExtensions: Set<string>,
 *      mimetypes: {},
 *      throttleRate: Number,
 *      mkvExtensionWorkaround: Boolean
 *    }}
 */
let config = defaultConfig;

try {
  const customConfig = (await import('../config.js')).default;
  config = {
    ...config,
    ...customConfig
  };

} catch {}

config.allowedExtensions = new Set(config.allowedExtensions)

export default config;
