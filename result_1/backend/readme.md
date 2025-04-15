### 25/4/16 기준 파일 다시 다운받으신 후, 실행하실 때 아래 참고해주세요.
backend 경로 터미널에서 MySQL 접속하신 후 (mysql -u root -p -> 비밀번호 입력)  
터미널창에 아래 쿼리문 입력하신 후 프로젝트 실행.  
> ALTER TABLE products ADD COLUMN filename VARCHAR(255);

### 테스트 시, 실시간 환율 API 적용을 비활성화 해두었습니다.  
하루에 API키 불러오는 횟수에 제한이 걸려 있어서, 테스트 시에는 비활성화 해두었습니다.  
활성화 방법은 .env 파일에서 TEST_MODE=false로 변경하시면 됩니다.  
(프론트는 const TEST_EXCHANGE_RATE = 1350; -> const TEST_EXCHANGE_RATE = null;로 변경)  
