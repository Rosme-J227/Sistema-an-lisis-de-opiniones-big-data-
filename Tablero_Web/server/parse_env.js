const fs = require('fs');
const dotenv = require('dotenv');
const p = __dirname + '/.env';
if (!fs.existsSync(p)) { console.error('.env not found'); process.exit(1); }
const parsed = dotenv.parse(fs.readFileSync(p));
console.log('PARSED .env ->', parsed);
