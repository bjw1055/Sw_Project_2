import React, { useState } from "react";
import axios from "axios";
import { Bar, Line, Pie } from "react-chartjs-2";
import {
    Chart as ChartJS,
    CategoryScale,
    LinearScale,
    BarElement,
    LineElement,
    PointElement,
    ArcElement,
    Tooltip,
    Legend,
} from "chart.js";
import "./App.css";

ChartJS.register(
    CategoryScale,
    LinearScale,
    BarElement,
    LineElement,
    PointElement,
    ArcElement,
    Tooltip,
    Legend
);

function App() {
    const [csvData, setCsvData] = useState([]);
    const [file, setFile] = useState(null);
    const [chartData, setChartData] = useState({});
    const [lineData, setLineData] = useState({});
    const [pieData, setPieData] = useState({});
    const [activeChart, setActiveChart] = useState("bar");
    const [exchangeRate, setExchangeRate] = useState(null);
    const [showAllRows, setShowAllRows] = useState(false);
    const displayData = showAllRows ? csvData : csvData.slice(0, 5);
    const totalKRW = csvData.reduce((sum, row) => sum + row["매출액(KRW)"], 0);
    const totalUSD = csvData.reduce((sum, row) => sum + row["매출액(USD)"], 0);
    const totalQty = csvData.reduce((sum, row) => sum + row["수량"], 0);
    const avgPrice = totalQty > 0 ? totalKRW / totalQty : 0;

    const handleFileChange = (e) => {
        setFile(e.target.files[0]);
    };

    const handleUpload = async () => {
        const formData = new FormData();
        formData.append("csv", file);

        try {
            console.log("업로드 버튼 클릭됨");
            const response = await axios.post("http://192.168.219.101:3001/upload", formData);
            console.log("응답도착", response.data);

            setCsvData(response.data.convertedData);
            setChartData(response.data.bar || {});
            setLineData(response.data.line || {});
            setPieData(response.data.pie || {});
            setExchangeRate(response.data.exchangeRate);
        } catch (error) {
            console.error("업로드 실패:", error);
        }
    };

    const renderChart = () => {
        if (activeChart === "bar" && chartData && chartData.labels && chartData.labels.length > 0) {
            return <Bar data={chartData} options={{ responsive: true }} />;
        } else if (activeChart === "line" && lineData && lineData.labels && lineData.labels.length > 0) {
            return <Line data={lineData} options={{ responsive: true }} />;
        } else if (activeChart === "pie" && pieData && pieData.labels && pieData.labels.length > 0) {
            return <Pie data={pieData} options={{ responsive: true }} />;
        }
        return null;
    };

    return (
        <div className="app">
            <h2>📁 CSV 업로드 및 환율 분석</h2>
            <input type="file" onChange={handleFileChange} />
            <button onClick={handleUpload}>업로드</button>

            {csvData.length > 0 && (
                <>
                    {exchangeRate && (
                        <p className="exchange-info">💲 현재 환율: 1 USD = {exchangeRate.toLocaleString()} KRW</p>
                    )}

                    <h3>📊 데이터 미리보기</h3>
                    <div className="table-wrapper">
                        <table className="custom-table">
                            <thead>
                                <tr>
                                    <th>날짜</th>
                                    <th>제품명</th>
                                    <th>수량</th>
                                    <th>가격</th>
                                    <th>매출액(KRW)</th>
                                    <th>매출액(USD)</th>
                                </tr>
                            </thead>
                            <tbody>
                                {displayData.map((row, index) => (
                                    <tr key={index}>
                                        <td>{row.날짜}</td>
                                        <td>{row.제품명}</td>
                                        <td>{row.수량}</td>
                                        <td>{row.가격.toLocaleString()}</td>
                                        <td>{row["매출액(KRW)"].toLocaleString()}</td>
                                        <td>{row["매출액(USD)"].toLocaleString()}</td>
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    </div>

                    <button onClick={() => setShowAllRows(!showAllRows)} className="toggle-table-button">
                        {showAllRows ? "간략히 보기 🔼" : "전체 보기 🔽"}
                    </button>

                    {csvData.length > 0 && (
                        <div className="stats-wrapper">
                            <div className="stat-card">💰 총 매출 (KRW)<br /><strong>{totalKRW.toLocaleString()} 원</strong></div>
                            <div className="stat-card">💵 총 매출 (USD)<br /><strong>{totalUSD.toLocaleString()} $</strong></div>
                            <div className="stat-card">📦 총 수량<br /><strong>{totalQty.toLocaleString()} 개</strong></div>
                            <div className="stat-card">💳 평균 단가<br /><strong>{avgPrice.toLocaleString()} 원</strong></div>
                        </div>
                    )}

                    <div className="chart-buttons">
                        <button onClick={() => setActiveChart("bar")}>📊 막대차트</button>
                        <button onClick={() => setActiveChart("line")}>📈 라인차트</button>
                        <button onClick={() => setActiveChart("pie")}>🍰 파이차트</button>
                    </div>

                    <div className="chart-container">{renderChart()}</div>
                </>
            )}
        </div>
    );
}

export default App;
