// 오늘 날짜의 전날(어제)을 기본값으로 하는 함수
function getDefaultDate() {
  const today = new Date();
  const yesterday = new Date(today);
  yesterday.setDate(today.getDate() - 1);
  // YYYY-MM-DD 형식으로 반환
  const yyyy = yesterday.getFullYear();
  const mm = String(yesterday.getMonth() + 1).padStart(2, '0');
  const dd = String(yesterday.getDate()).padStart(2, '0');
  return `${yyyy}-${mm}-${dd}`;
}

// 페이지 로드 시, 날짜 입력란 기본값 설정.
window.addEventListener('load', () => {
  const defaultDate = getDefaultDate();
  const startDateInput = document.getElementById('start-date');
  const endDateInput = document.getElementById('end-date');
  if (startDateInput) startDateInput.value = defaultDate;
  if (endDateInput) endDateInput.value = defaultDate;
});

async function generateReport() {
  const startDate = document.getElementById('start-date').value;
  const endDate = document.getElementById('end-date').value;
  const password = document.getElementById('report-password').value;

  // 패스워드 입력 확인
  if (!password) {
    alert("패스워드를 입력해주세요.");
    return;
  }

  // 날짜 입력 확인
  if (!startDate || !endDate) {
    alert("시작 날짜와 종료 날짜를 모두 입력해주세요.");
    return;
  }

  document.getElementById('error-message').textContent = '보고서를 생성 중입니다...';

  try {
    const response = await fetch('/api/generate-report', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        start_date: startDate,
        end_date: endDate,
        password: password
      })
    });
    
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }
    
    const data = await response.json();
    if (data.error) {
      alert(data.error);
      document.getElementById('error-message').textContent = `오류 발생: ${data.error}`;
      return;
    }
    
    document.getElementById('error-message').textContent = '';
    renderReport(data);
  } catch (error) {
    console.error("API 호출 중 오류:", error);
    alert("API 호출 중 오류 발생: " + error.message);
  }
}

// 예시 renderReport: 응답받은 HTML 테이블을 그대로 화면에 출력
function renderReport(data) {
  const reportResult = document.getElementById('report-result');
  if (data.html_table) {
    reportResult.innerHTML = data.html_table;
  } else {
    reportResult.textContent = JSON.stringify(data, null, 2);
  }
}
