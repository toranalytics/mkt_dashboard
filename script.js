function getDefaultDate() {
  const today = new Date();
  const yesterday = new Date(today);
  yesterday.setDate(today.getDate() - 1);
  const yyyy = yesterday.getFullYear();
  const mm = String(yesterday.getMonth() + 1).padStart(2, '0');
  const dd = String(yesterday.getDate()).padStart(2, '0');
  return `${yyyy}-${mm}-${dd}`;
}

async function setupAccountDropdown() {
  const passwordInput = document.getElementById('report-password');
  const password = passwordInput ? passwordInput.value : null;
  const container = document.getElementById('account-selector-container');
  if (!container) return;
  container.innerHTML = '<label for="account-dropdown" style="margin-right: 5px;">광고 계정 선택:</label>';
  if (!password) {
    const placeholder = document.createElement('span');
    placeholder.textContent = ' (비밀번호를 입력하면 계정 목록을 불러옵니다)';
    placeholder.style.color = '#888';
    container.appendChild(placeholder);
    if (passwordInput && !passwordInput.dataset.listenerAttached) {
        passwordInput.addEventListener('change', setupAccountDropdown);
        passwordInput.dataset.listenerAttached = 'true';
    }
    return;
  }
  try {
    const response = await fetch('/api/accounts', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ password: password })
    });
    if (!response.ok) {
      let errorMsg = `HTTP error! status: ${response.status}`;
      try { const errorData = await response.json(); errorMsg = errorData.error || errorMsg; } catch (e) {}
      throw new Error(errorMsg);
    }
    const accountNames = await response.json();
    if (accountNames && accountNames.length > 0) {
      const select = document.createElement('select');
      select.id = 'account-dropdown';
      select.name = 'account-dropdown';
      select.style.padding = '5px';
      select.style.marginLeft = '5px';
      accountNames.forEach(name => {
        const option = document.createElement('option');
        option.value = name;
        option.textContent = name;
        select.appendChild(option);
      });
      container.appendChild(select);
    } else {
      const noAccountsMsg = document.createElement('span');
      noAccountsMsg.textContent = ' 로드된 광고 계정이 없습니다.';
      container.appendChild(noAccountsMsg);
    }
  } catch (error) {
    const errorSpan = document.createElement('span');
    errorSpan.textContent = ` 계정 목록 로딩 실패: ${error.message}`;
    errorSpan.style.color = 'red';
    container.appendChild(errorSpan);
  }
}

window.addEventListener('load', () => {
  const defaultDate = getDefaultDate();
  const startDateInput = document.getElementById('start-date');
  const endDateInput = document.getElementById('end-date');
  if (startDateInput) startDateInput.value = defaultDate;
  if (endDateInput) endDateInput.value = defaultDate;
  setupAccountDropdown();
});

async function generateReport() {
  const startDate = document.getElementById('start-date').value;
  const endDate = document.getElementById('end-date').value;
  const password = document.getElementById('report-password').value;
  const accountDropdown = document.getElementById('account-dropdown');
  let selectedAccountKey = null;
  if (accountDropdown) selectedAccountKey = accountDropdown.value;
  if (!password) { alert("패스워드를 입력해주세요."); return; }
  if (!startDate || !endDate) { alert("시작 날짜와 종료 날짜를 모두 입력해주세요."); return; }
  if (!accountDropdown || !selectedAccountKey) {
    alert("광고 계정을 선택해주세요. (계정 목록이 보이지 않으면 비밀번호를 확인 후 다시 시도해주세요)");
    setupAccountDropdown();
    return;
  }
  document.getElementById('error-message').textContent = '보고서를 생성 중입니다...';
  document.getElementById('report-result').innerHTML = '';
  try {
    const response = await fetch('/api/generate-report', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        start_date: startDate,
        end_date: endDate,
        password: password,
        selected_account_key: selectedAccountKey
      })
    });
    if (!response.ok) {
      let errorMsg = `HTTP error! status: ${response.status}`;
      try { const errorData = await response.json(); errorMsg = errorData.error || errorMsg; } catch (e) {}
      throw new Error(errorMsg);
    }
    const data = await response.json();
    if (data.error) {
      alert(`보고서 생성 오류: ${data.error}`);
      document.getElementById('error-message').textContent = `오류 발생: ${data.error}`;
      return;
    }
    document.getElementById('error-message').textContent = '';
    renderReport(data);
    enableTableSort();
  } catch (error) {
    alert("서버 호출 중 오류 발생: " + error.message);
    document.getElementById('error-message').textContent = `오류: ${error.message}`;
  }
}

function renderReport(data) {
  const reportResult = document.getElementById('report-result');
  if (data.html_table) {
    reportResult.innerHTML = data.html_table;
  } else {
    reportResult.innerHTML = `<pre>${JSON.stringify(data, null, 2)}</pre>`;
  }
}

// 테이블 정렬 기능 + 아이콘, 광고 성과 우선순위 정렬, 상단 컬럼 고정
function enableTableSort() {
  const table = document.querySelector("#report-result table");
  if (!table) return;
  let currentSortCol = null;
  let currentSortAsc = true;
  const ths = table.querySelectorAll("th");
  ths.forEach((th, idx) => {
    th.addEventListener('click', () => sortTable(idx));
  });
  function sortTable(colIndex) {
    const tbody = table.tBodies[0];
    const rows = Array.from(tbody.rows);
    // 합계 행은 항상 맨 위에 고정 (클래스명으로 구분)
    const fixedRows = rows.filter(r => r.classList.contains('total-row'));
    const dataRows = rows.filter(r => !r.classList.contains('total-row'));
    if (currentSortCol === colIndex) currentSortAsc = !currentSortAsc;
    else { currentSortCol = colIndex; currentSortAsc = true; }
    dataRows.sort((a, b) => {
      let x = a.cells[colIndex].textContent.trim();
      let y = b.cells[colIndex].textContent.trim();
      // 광고 성과 우선순위
      if (ths[colIndex].textContent.includes("광고 성과")) {
        const perfRank = {'위닝 콘텐츠': 1, '고성과 콘텐츠': 2, '성과 콘텐츠': 3, '개선 필요!': 4, '': 5};
        return currentSortAsc ? perfRank[x] - perfRank[y] : perfRank[y] - perfRank[x];
      }
      let xNum = parseFloat(x.replace(/[^0-9.-]+/g,""));
      let yNum = parseFloat(y.replace(/[^0-9.-]+/g,""));
      if (!isNaN(xNum) && !isNaN(yNum)) return currentSortAsc ? xNum - yNum : yNum - xNum;
      return currentSortAsc ? x.localeCompare(y) : y.localeCompare(x);
    });
    tbody.innerHTML = "";
    fixedRows.forEach(row => tbody.appendChild(row));
    dataRows.forEach(row => tbody.appendChild(row));
    ths.forEach(iconTh => {
      const icon = iconTh.querySelector('.sort-icon');
      if (icon) icon.textContent = "";
    });
    const icon = ths[colIndex].querySelector('.sort-icon');
    if (icon) icon.textContent = currentSortAsc ? "▲" : "▼";
  }
}
