const axios = require('axios');
axios.get('http://localhost:8585/api/dataloader/transaction/get/all', {
  headers: { 'X-Tenant': '19btech' }
}).then(res => console.log("DATA:", res.data)).catch(err => console.error(err.message));
