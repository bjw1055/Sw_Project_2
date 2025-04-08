const express = require('express');
const router = express.Router();
const multer = require('multer');
const path = require('path');
const dataController = require('../controllers/dataController');

// multer 설정
const storage = multer.diskStorage({
  destination: './uploads/',
  filename: (req, file, cb) => {
    cb(null, `upload-${Date.now()}${path.extname(file.originalname)}`);
  },
});
const upload = multer({ storage });

// CSV 업로드
router.post('/upload', upload.single('file'), dataController.uploadCSV);

// 데이터 조회
router.get('/data', dataController.getData);

// 예측 결과
router.get('/predict', dataController.getPrediction);

router.get('/data-with-outliers', dataController.getDataWithOutliers);

module.exports = router;
