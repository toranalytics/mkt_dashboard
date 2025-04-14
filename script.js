// 오늘 날짜의 전날(어제)을 기본값으로 하는 함수
function getDefaultDate() {
  const today = new Date();
  const yesterday = new Date(today);
  yesterday.setDate(today.getDate() - 1);
  // YYYY-MM-DD 형식으로 반환
  const yyyy = yesterday.getFullYear();
  const mm = String(yesterday.getMonth() + 1).padStart(2, '0'); // 월은 0부터 시작하므로 +1
  const dd = String(yesterday.getDate()).padStart(2, '0');
  return `${yyyy}-${mm}-${dd}`;
}

// 계정 목록을 가져와 드롭다운을 생성하는 함수
async function setupAccountDropdown() {
  const passwordInput = document.getElementById('report-password');
  // 페이지 로드 시점에 비밀번호 필드가 채워져 있지 않을 수 있음
  const password = passwordInput ? passwordInput.value : null;
  const container = document.getElementById('account-selector-container'); // HTML에 이 ID를 가진 요소 필요

  if (!container) {
    console.error("HTML에서 ID 'account-selector-container'를 찾을 수 없습니다.");
    return; // 컨테이너 없으면 함수 종료
  }

  // 이전 드롭다운이나 메시지 삭제 후 라벨 추가
  container.innerHTML = '<label for="account-dropdown" style="margin-right: 5px;">광고 계정 선택:</label>';

  // 계정 목록을 가져오려면 비밀번호가 필요함
  if (!password) {
    const placeholder = document.createElement('span');
    placeholder.textContent = ' (비밀번호를 입력하면 계정 목록을 불러옵니다)';
    placeholder.style.color = '#888'; // 약간 흐린 색상
    container.appendChild(placeholder);
    // 비밀번호 필드에 이벤트 리스너 추가 (비밀번호 입력 시 드롭다운 다시 로드 시도)
    if (passwordInput && !passwordInput.dataset.listenerAttached) {
        passwordInput.addEventListener('change', setupAccountDropdown);
        passwordInput.dataset.listenerAttached = 'true'; // 리스너 중복 부착 방지
    }
    return; // 비밀번호 없으면 여기서 종료
  }

  try {
    console.log("Fetching account list...");
    const response = await fetch('/api/accounts', {
      method: 'POST', // 백엔드에서 POST로 받도록 설정했으므로 POST 사용
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ password: password }) // 비밀번호 전송
    });

    if (!response.ok) {
      let errorMsg = `HTTP error! status: ${response.status}`;
      try {
        const errorData = await response.json(); // 오류 메시지 파싱 시도
        errorMsg = errorData.error || errorMsg;
      } catch (e) { /* 파싱 실패 시 기존 메시지 사용 */ }
      throw new Error(errorMsg);
    }

    const accountNames = await response.json();

    if (accountNames && accountNames.length > 0) {
      const select = document.createElement('select');
      select.id = 'account-dropdown';
      select.name = 'account-dropdown';
      select.style.padding = '5px'; // 약간의 스타일링 추가
      select.style.marginLeft = '5px';

      accountNames.forEach(name => {
        const option = document.createElement('option');
        option.value = name; // value를 계정 이름으로 설정
        option.textContent = name;
        select.appendChild(option);
      });
      container.appendChild(select); // 생성된 드롭다운을 컨테이너에 추가
      console.log("Account dropdown created successfully.");
    } else {
      const noAccountsMsg = document.createElement('span');
      noAccountsMsg.textContent = ' 로드된 광고 계정이 없습니다.';
      container.appendChild(noAccountsMsg);
      console.log("No accounts returned from API.");
    }

  } catch (error) {
    console.error("Error fetching or creating account dropdown:", error);
    const errorSpan = document.createElement('span');
    errorSpan.textContent = ` 계정 목록 로딩 실패: ${error.message}`;
    errorSpan.style.color = 'red';
    container.appendChild(errorSpan);
  }
}

// 페이지 로드 시 실행
window.addEventListener('load', () => {
  // 날짜 기본값 설정
  const defaultDate = getDefaultDate();
  const startDateInput = document.getElementById('start-date');
  const endDateInput = document.getElementById('end-date');
  if (startDateInput) startDateInput.value = defaultDate;
  if (endDateInput) endDateInput.value = defaultDate;

  // 계정 드롭다운 설정 시도 (페이지 로드 시 비밀번호가 입력되어 있다면 바로 로드됨)
  setupAccountDropdown();
});

// 보고서 생성 버튼 클릭 시 실행될 함수 (수정됨)
async function generateReport() {
  const startDate = document.getElementById('start-date').value;
  const endDate = document.getElementById('end-date').value;
  const password = document.getElementById('report-password').value;
  const accountDropdown = document.getElementById('account-dropdown'); // 드롭다운 요소 가져오기

  // 선택된 계정 이름 가져오기
  let selectedAccountKey = null;
  if (accountDropdown) {
    selectedAccountKey = accountDropdown.value;
  }

  // 입력값 유효성 검사
  if (!password) {
    alert("패스워드를 입력해주세요.");
    return;
  }
  if (!startDate || !endDate) {
    alert("시작 날짜와 종료 날짜를 모두 입력해주세요.");
    return;
  }
  // 계정이 선택되었는지 확인 (드롭다운이 존재하고 값이 있는지)
  if (!accountDropdown || !selectedAccountKey) {
    alert("광고 계정을 선택해주세요. (계정 목록이 보이지 않으면 비밀번호를 확인 후 다시 시도해주세요)");
    // 혹시 비밀번호를 방금 입력했다면 드롭다운 리로드 시도
    setupAccountDropdown();
    return;
  }

  // 로딩 메시지 표시 및 이전 결과 초기화
  document.getElementById('error-message').textContent = '보고서를 생성 중입니다...';
  document.getElementById('report-result').innerHTML = '';

  try {
    const response = await fetch('/api/generate-report', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      // 요청 본문에 selected_account_key 추가!
      body: JSON.stringify({
        start_date: startDate,
        end_date: endDate,
        password: password,
        selected_account_key: selectedAccountKey // 선택된 계정 이름 전달
      })
    });

    // 응답 상태 먼저 확인
    if (!response.ok) {
      let errorMsg = `HTTP error! status: ${response.status}`;
      try {
        const errorData = await response.json(); // 백엔드 에러 메시지 파싱 시도
        errorMsg = errorData.error || errorMsg;
      } catch (e) { /* 파싱 실패 시 무시 */ }
      throw new Error(errorMsg); // 에러 발생시키기 (catch 블록으로 이동)
    }

    // 응답 성공 시 JSON 파싱
    const data = await response.json();

    // 백엔드 로직 내 에러 처리 (response.ok 이지만 data.error 가 있는 경우)
    if (data.error) {
      alert(`보고서 생성 오류: ${data.error}`);
      document.getElementById('error-message').textContent = `오류 발생: ${data.error}`;
      return;
    }

    // 성공 시 로딩/에러 메시지 지우고 결과 렌더링
    document.getElementById('error-message').textContent = '';
    renderReport(data);

  } catch (error) {
    // 네트워크 오류 또는 throw new Error()로 발생시킨 오류 처리
    console.error("API 호출 중 오류:", error);
    alert("서버 호출 중 오류 발생: " + error.message); // 구체적인 에러 메시지 표시
    document.getElementById('error-message').textContent = `오류: ${error.message}`;
  }
}

// 보고서 렌더링 함수 (기존과 동일)
function renderReport(data) {
  const reportResult = document.getElementById('report-result');
  if (data.html_table) {
    reportResult.innerHTML = data.html_table;
  } else {
    // HTML 테이블이 없는 경우, JSON 데이터를 보기 좋게 표시 (예: 오류 응답)
    reportResult.innerHTML = `<pre>${JSON.stringify(data, null, 2)}</pre>`;
  }
}
