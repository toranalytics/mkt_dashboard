async function generateReport() {
  const startDate = document.getElementById('start-date').value;
  const endDate = document.getElementById('end-date').value;

  if (!startDate || !endDate) {
    document.getElementById('error-message').textContent = '시작 날짜와 종료 날짜를 모두 입력해주세요.';
    return;
  }

  document.getElementById('error-message').textContent = '보고서를 생성 중입니다...';

  try {
    const response = await fetch('/api/generate-report', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ start_date: startDate, end_date: endDate }),
    });
    
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    const data = await response.json(); // { html_table, data }

    if (data.error) {
      document.getElementById('error-message').textContent = `오류 발생: ${data.error}`;
      return;
    }

    document.getElementById('error-message').textContent = '';
    // 여기서 renderReport 함수를 호출
    renderReport(data);

  } catch (error) {
    console.error("API 호출 오류:", error);
    document.getElementById('error-message').textContent = `오류 발생: ${error.message}`;
  }
}

// renderReport 함수 재정의
function renderReport(data) {
  // data에는 서버에서 온 { html_table, data } 등이 들어있음
  // html_table이 있으면 그대로 표시하고, 없으면 data를 JSON으로 확인
  const reportResult = document.getElementById('report-result');
  
  if (data.html_table) {
    reportResult.innerHTML = data.html_table;
  } else {
    // html_table이 없으면 테이블 대신 raw JSON을 출력
    reportResult.textContent = JSON.stringify(data, null, 2);
  }
}
