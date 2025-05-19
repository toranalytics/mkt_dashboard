document.addEventListener("DOMContentLoaded", function() {
  const passwordInput = document.getElementById("password");
  const accountSelect = document.getElementById("selected_account_key");
  const startDateInput = document.getElementById("start_date");
  const endDateInput = document.getElementById("end_date");
  const reportForm = document.getElementById("reportForm");
  const loadingDiv = document.getElementById("loading");
  const resultDiv = document.getElementById("result");

  // 어제 날짜를 기본값으로 설정
  function setDefaultDate() {
    const today = new Date();
    const yesterday = new Date(today);
    yesterday.setDate(today.getDate() - 1);
    const ymd = d => d.toISOString().slice(0,10);
    startDateInput.value = ymd(yesterday);
    endDateInput.value = ymd(yesterday);
  }

  setDefaultDate();

  // 비밀번호 입력 시 계정 목록 불러오기
  passwordInput.addEventListener("blur", function() {
    const pw = passwordInput.value.trim();
    if (!pw) return;
    fetch("/api/accounts", {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify({ password: pw })
    })
    .then(res => res.ok ? res.json() : Promise.reject())
    .then(data => {
      accountSelect.innerHTML = `<option value="">계정 선택</option>`;
      data.forEach(name => {
        accountSelect.innerHTML += `<option value="${name}">${name}</option>`;
      });
    })
    .catch(() => {
      accountSelect.innerHTML = `<option value="">계정 선택</option>`;
    });
  });

  // 폼 제출 시 보고서 생성
  reportForm.addEventListener("submit", function(e) {
    e.preventDefault();
    const pw = passwordInput.value.trim();
    const accountKey = accountSelect.value;
    const startDate = startDateInput.value;
    const endDate = endDateInput.value;

    if (!pw || !accountKey || !startDate || !endDate) {
      alert("모든 항목을 입력해 주세요.");
      return;
    }

    loadingDiv.style.display = "block";
    resultDiv.innerHTML = "";

    fetch("/api/generate-report", {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify({
        password: pw,
        selected_account_key: accountKey,
        start_date: startDate,
        end_date: endDate
      })
    })
    .then(res => res.json())
    .then(data => {
      loadingDiv.style.display = "none";
      if (data.error) {
        resultDiv.innerHTML = `<div class="error">${data.error}</div>`;
      } else if (data.html_table) {
        resultDiv.innerHTML = data.html_table;
      } else {
        resultDiv.innerHTML = "<p>결과가 없습니다.</p>";
      }
    })
    .catch(err => {
      loadingDiv.style.display = "none";
      resultDiv.innerHTML = "<div class='error'>보고서 생성 중 오류가 발생했습니다.</div>";
    });
  });
});
