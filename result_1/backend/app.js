const express = require('express');
const cors = require('cors');
const app = express();

require('dotenv').config();

const dataRoutes = require('./routes/dataRoutes');
const authRoutes = require('./routes/authRoutes');


app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

app.use('/api', dataRoutes);
app.use('/api/auth', authRoutes);

const PORT = process.env.PORT || 5000;
app.listen(PORT, () => {
  console.log(`🚀 서버가 포트 ${PORT}에서 실행 중입니다`);
});

module.exports = app;