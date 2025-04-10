// controllers/dataController.js
const fs = require('fs');
const csv = require('csv-parser');
const db = require('../db');
const { exec } = require('child_process');

// 1. CSV 업로드
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

// 2. 전체 데이터 조회 (날짜 범위 포함)
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

// 3. 이상치 탐지
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

// 4. 예측 결과 실행
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

// 5. 제품별 카테고리 요약
exports.getCategorySummary = (req, res) => {
  const sql = 'SELECT name AS category, SUM(amount) AS total FROM products GROUP BY name';

  db.query(sql, (err, results) => {
    if (err) {
      console.error('카테고리 요약 실패:', err);
      return res.status(500).json({ error: '카테고리 요약 실패' });
    }
    res.json(results);
  });
};

// 6. 데이터 검색 (정확한 이름)
exports.searchByName = (req, res) => {
  const { name } = req.query;
  if (!name) {
    return res.status(400).json({ error: '검색어가 필요합니다.' });
  }
  const sql = 'SELECT * FROM products WHERE name LIKE ?';
  db.query(sql, [`%${name}%`], (err, results) => {
    if (err) {
      console.error('검색 실패:', err);
      return res.status(500).json({ error: '검색 실패' });
    }
    res.json(results);
  });
};

// 7. 데이터 삭제
exports.deleteByName = (req, res) => {
  const { name } = req.body;
  const sql = 'DELETE FROM products WHERE name = ?';

  db.query(sql, [name], (err, result) => {
    if (err) {
      console.error('삭제 실패:', err);
      return res.status(500).json({ error: '삭제 실패' });
    }
    res.json({ message: '삭제 성공', affected: result.affectedRows });
  });
};

// 8. 데이터 수정
exports.updateAmount = (req, res) => {
  const { name, amount } = req.body;

  if (!name || !amount) {
    return res.status(400).json({ error: '이름과 금액이 필요합니다.' });
  }

  const sql = 'UPDATE products SET amount = ? WHERE name = ?';
  db.query(sql, [amount, name], (err, result) => {
    if (err) {
      console.error('수정 실패:', err);
      return res.status(500).json({ error: '수정 실패' });
    }
    res.json({ message: '수정 완료', affected: result.affectedRows });
  });
};