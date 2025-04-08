import React, { useState, useEffect } from 'react';
import './App.css';
import { Line } from 'react-chartjs-2';
import {
  Chart as ChartJS,
  LineElement,
  CategoryScale,
  LinearScale,
  PointElement,
  Title,
  Tooltip,
  Legend,
} from 'chart.js';

ChartJS.register(
  LineElement,
  CategoryScale,
  LinearScale,
  PointElement,
  Title,
  Tooltip,
  Legend
);

function App() {
  const [file, setFile] = useState(null);
  const [message, setMessage] = useState('');
  const [chartData, setChartData] = useState(null);
  const [outlierData, setOutlierData] = useState(null);
  const [predictionData, setPredictionData] = useState(null);
  const [startDate, setStartDate] = useState('');
  const [endDate, setEndDate] = useState('');
  const [hasUploaded, setHasUploaded] = useState(false); // ✅ 업로드 상태 추가

  const handleFileChange = (e) => {
    setFile(e.target.files[0]);
  };

  const handleUpload = async () => {
    if (!file) {
      setMessage('CSV 파일을 선택해주세요.');
      return;
    }

    const formData = new FormData();
    formData.append('file', file);

    try {
      const response = await fetch('http://localhost:5000/api/upload', {
        method: 'POST',
        body: formData,
      });

      const result = await response.json();
      if (response.ok) {
        setMessage(`✅ ${result.message} (총 ${result.inserted}개 삽입됨)`);
        setHasUploaded(true); // ✅ 업로드 성공 상태로 변경
        fetchAll();
      } else {
        setMessage(`❌ 오류: ${result.error}`);
      }
    } catch (err) {
      console.error(err);
      setMessage('❌ 서버와의 통신 중 문제가 발생했습니다.');
    }
  };

  const fetchAll = () => {
    fetchData(startDate, endDate);
    fetchOutliers();
    fetchPrediction();
  };

  const fetchData = async (start, end) => {
    let url = 'http://localhost:5000/api/data';
    if (start && end) {
      url += `?start=${start}&end=${end}`;
    }

    const response = await fetch(url);
    const data = await response.json();

    const labels = data.map(item => item.date.split('T')[0]);
    const values = data.map(item => item.amount);

    setChartData({
      labels,
      datasets: [
        {
          label: '실제 매출',
          data: values,
          borderColor: 'rgb(75, 192, 192)',
          tension: 0.3,
          fill: false,
        },
      ],
    });
  };

  const fetchOutliers = async () => {
    const response = await fetch('http://localhost:5000/api/data-with-outliers');
    const data = await response.json();

    const labels = data.map(item => item.date);
    const amounts = data.map(item => item.amount);
    const isOutlier = data.map(item => item.outlier);

    setOutlierData({
      labels,
      datasets: [
        {
          label: '이상치 포함 매출',
          data: amounts,
          borderColor: 'rgba(255, 99, 132, 0.5)',
          pointBackgroundColor: isOutlier.map(o => (o ? 'red' : 'rgba(255,99,132,0.3)')),
          pointRadius: isOutlier.map(o => (o ? 6 : 3)),
          tension: 0.3,
        },
      ],
    });
  };

  const fetchPrediction = async () => {
    const response = await fetch('http://localhost:5000/api/predict');
    const data = await response.json();

    const labels = data.forecast.map(item => item.date);
    const values = data.forecast.map(item => item.predicted);

    setPredictionData({
      labels,
      datasets: [
        {
          label: '예측 매출',
          data: values,
          borderColor: 'rgba(255, 99, 132, 1)',
          tension: 0.4,
          fill: false,
        },
      ],
    });
  };

  return (
    <div className="container">
      <h1>CSV 파일 업로드</h1>
      <input type="file" accept=".csv" onChange={handleFileChange} />
      <button onClick={handleUpload}>업로드</button>
      {message && <p>{message}</p>}

      {/* 날짜 범위 필터 */}
      {hasUploaded && (
        <div style={{ margin: '20px 0' }}>
          <label>시작일: </label>
          <input
            type="date"
            value={startDate}
            onChange={(e) => setStartDate(e.target.value)}
          />
          <label style={{ marginLeft: '10px' }}>종료일: </label>
          <input
            type="date"
            value={endDate}
            onChange={(e) => setEndDate(e.target.value)}
          />
          <button onClick={fetchAll} style={{ marginLeft: '10px' }}>
            필터 적용
          </button>
        </div>
      )}

      {hasUploaded && chartData && (
        <div>
          <h2>📊 실제 매출 시각화</h2>
          <Line data={chartData} />
        </div>
      )}

      {hasUploaded && outlierData && (
        <div style={{ marginTop: '30px' }}>
          <h2>🚨 이상치 탐지 시각화</h2>
          <Line data={outlierData} />
        </div>
      )}

      {hasUploaded && predictionData && (
        <div style={{ marginTop: '30px' }}>
          <h2>🔮 예측 매출 시각화</h2>
          <Line data={predictionData} />
        </div>
      )}
    </div>
  );
}

export default App;
