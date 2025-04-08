const fs = require('fs');
const csv = require('csv-parser');
const db = require('../db');
const { exec } = require('child_process');

// CSV 업로드
exports.uploadCSV = (req, res) => {
  const results = [];
  const filePath = req.file.path;

  fs.createReadStream(filePath)
    .pipe(csv())
    .on('data', (data) => results.push(data))
    .on('end', () => {
      const insertQuery = 'INSERT INTO products (name, quantity, amount, date) VALUES ?';
      const values = results.map((row) => [
        row.name,
        Number(row.quantity),
        Number(row.amount),
        row.date,
      ]);

      db.query(insertQuery, [values], (err, result) => {
        if (err) {
          console.error('DB 삽입 오류:', err);
          return res.status(500).json({ error: 'DB 오류' });
        }
        res.json({ message: 'CSV 업로드 성공', inserted: result.affectedRows });
      });
    });
};

// 날짜 범위 지원 + 전체 데이터 조회
exports.getData = (req, res) => {
  const { start, end } = req.query;
  let sql = 'SELECT * FROM products';
  const values = [];

  if (start && end) {
    sql += ' WHERE date BETWEEN ? AND ? ORDER BY date ASC';
    values.push(start, end);
  } else {
    sql += ' ORDER BY date ASC';
  }

  db.query(sql, values, (err, results) => {
    if (err) {
      console.error('DB 조회 실패:', err);
      return res.status(500).json({ error: 'DB 조회 실패' });
    }
    res.json(results);
  });
};

// 이상치 탐지용 API
exports.getDataWithOutliers = (req, res) => {
  const sql = 'SELECT date, amount FROM products ORDER BY date ASC';
  db.query(sql, (err, results) => {
    if (err) {
      console.error('DB 조회 실패:', err);
      return res.status(500).json({ error: 'DB 조회 실패' });
    }

    const values = results.map(r => r.amount);
    const avg = values.reduce((a, b) => a + b, 0) / values.length;
    const threshold = avg * 1.5;

    const dataWithOutliers = results.map(row => ({
      date: row.date,
      amount: row.amount,
      outlier: row.amount > threshold || row.amount < avg / 1.5,
    }));

    res.json(dataWithOutliers);
  });
};

// 예측 결과
exports.getPrediction = (req, res) => {
  const scriptPath = 'predict.py';

  exec(`python ${scriptPath}`, (error, stdout, stderr) => {
    if (error) {
      console.error(`Python 실행 오류: ${error.message}`);
      return res.status(500).json({ error: '예측 실행 오류' });
    }
    if (stderr) {
      console.error(`Python stderr: ${stderr}`);
    }

    try {
      const output = JSON.parse(stdout);
      res.json(output);
    } catch (err) {
      console.error('예측 결과 파싱 실패:', err);
      res.status(500).json({ error: '예측 결과 파싱 실패' });
    }
  });
};
