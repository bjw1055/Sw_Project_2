const express = require('express');
const router = express.Router();
const multer = require('multer');
const upload = multer({ dest: 'uploads/' });
const dataController = require('../controllers/dataController');

// CSV 업로드
router.post('/upload', upload.single('file'), dataController.uploadCSV);

// 전체 데이터 조회 (날짜 범위 포함)
router.get('/data', dataController.getData);

// 이상치 포함 데이터 조회
router.get('/data-with-outliers', dataController.getDataWithOutliers);

// 예측 결과
router.get('/predict', dataController.getPrediction);

// 카테고리 요약
router.get('/category-summary', dataController.getCategorySummary);

// 제품명으로 검색
router.get('/search', dataController.searchByName);

// 제품 삭제
router.delete('/delete', dataController.deleteByName);

// 제품 금액 수정
router.put('/update', dataController.updateAmount);

module.exports = router;