require('dotenv').config({ path: __dirname + '/.env' });
console.log('DEBUG_ENV DB_USER => [' + (process.env.DB_USER || 'undefined') + ']');
console.log('DEBUG_ENV USE_WINDOWS_AUTH => [' + (process.env.USE_WINDOWS_AUTH || 'undefined') + ']');
