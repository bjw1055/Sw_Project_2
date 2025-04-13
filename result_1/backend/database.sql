-- DB 생성
CREATE DATABASE IF NOT EXISTS dashboard_db;
USE dashboard_db;

-- 👤 사용자 테이블
CREATE TABLE users (
  id INT AUTO_INCREMENT PRIMARY KEY,
  email VARCHAR(100) UNIQUE NOT NULL,
  password_hash VARCHAR(255) NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 📂 분석 항목 테이블 (사용자별 분석 프로젝트)
CREATE TABLE analysis_projects (
  id INT AUTO_INCREMENT PRIMARY KEY,
  user_id INT NOT NULL,
  title VARCHAR(100),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

-- 📦 업로드된 매출 데이터 테이블
CREATE TABLE products (
  id INT AUTO_INCREMENT PRIMARY KEY,
  project_id INT,
  raw_data JSON,
  uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (project_id) REFERENCES analysis_projects(id) ON DELETE CASCADE
);