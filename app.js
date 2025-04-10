const express = require('express');
const cors = require('cors');
const dotenv = require('dotenv');
const dataRoutes = require('./routes/dataRoutes');

dotenv.config();
const app = express();
app.use(cors());
app.use(express.json());

app.use('/api', dataRoutes);

const PORT = process.env.PORT || 5000;
app.listen(PORT, () => {
  console.log(`✅ 서버가 포트 ${PORT}에서 실행 중입니다`);
});
