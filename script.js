document.addEventListener("DOMContentLoaded", function() {
  const passwordInput = document.getElementById("password");
  const accountSelect = document.getElementById("selected_account_key");
  const startDateInput = document.getElementById("start_date");
  const endDateInput = document.getElementById("end_date");
  const reportForm = document.getElementById("reportForm");
  const loadingDiv = document.getElementById("loading");
  const resultDiv = document.getElementById("result");
  const paginationControlsDiv = document.getElementById("pagination-controls"); // 페이지네이션 UI 영역

  let currentPage = 1; // 현재 페이지 상태

  // 어제 날짜를 기본값으로 설정
  function setDefaultDate() {
    const today = new Date();
    const yesterday = new Date(today);
    yesterday.setDate(today.getDate() - 1);
    const ymd = d => d.toISOString().slice(0,10);
    startDateInput.value = ymd(yesterday);
    endDateInput.value = ymd(yesterday);
  }

  setDefaultDate(); //

  // 비밀번호 입력 시 계정 목록 불러오기
  passwordInput.addEventListener("blur", function() {
    const pw = passwordInput.value.trim();
    if (!pw) return;
    // 기존 계정 목록 UI 초기화
    accountSelect.innerHTML = `<option value="">계정 로딩 중...</option>`; 
    fetch("/api/accounts", {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify({ password: pw })
    })
    .then(res => {
      if (!res.ok) {
        // 오류 응답 처리 (예: 비밀번호 틀림)
        if (res.status === 403) {
          return res.json().then(errData => Promise.reject(new Error(errData.error || "비밀번호가 올바르지 않습니다.")));
        }
        return Promise.reject(new Error(`서버 오류: ${res.status}`));
      }
      return res.json();
    })
    .then(data => {
      accountSelect.innerHTML = `<option value="">계정 선택</option>`;
      if (Array.isArray(data)) {
        data.forEach(name => {
          accountSelect.innerHTML += `<option value="${name}">${name}</option>`;
        });
      } else {
        // 예상치 못한 데이터 형식 처리
         accountSelect.innerHTML = `<option value="">계정 목록 로드 실패</option>`;
         console.error("Received non-array data for accounts:", data);
      }
    })
    .catch(err => {
      console.error("Error fetching accounts:", err);
      accountSelect.innerHTML = `<option value="">계정 로드 실패</option>`;
      alert(err.message || "계정 목록을 불러오는 데 실패했습니다.");
    });
  }); //

  // 보고서 생성 함수 (페이지 번호 인자 추가)
  function generateReport(page = 1) {
    const pw = passwordInput.value.trim();
    const accountKey = accountSelect.value;
    const startDate = startDateInput.value;
    const endDate = endDateInput.value;

    if (!pw || !accountKey || !startDate || !endDate) {
      alert("모든 항목을 입력해 주세요.");
      return;
    }

    loadingDiv.style.display = "block";
    resultDiv.innerHTML = ""; // 이전 결과 초기화
    paginationControlsDiv.innerHTML = ""; // 이전 페이지네이션 UI 초기화

    fetch("/api/generate-report", {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify({
        password: pw,
        selected_account_key: accountKey,
        start_date: startDate,
        end_date: endDate,
        page: page // 페이지 번호 전달
      })
    })
    .then(res => {
        if (!res.ok) {
            return res.json().then(errData => Promise.reject(new Error(errData.error || `보고서 생성 오류 (${res.status})`)));
        }
        return res.json();
    })
    .then(data => {
      loadingDiv.style.display = "none";
      if (data.error) {
        resultDiv.innerHTML = `<div class="error" style="color: red; text-align: center; padding: 10px;">${data.error}</div>`;
      } else if (data.html_table && data.pagination) {
        resultDiv.innerHTML = data.html_table;
        currentPage = data.pagination.current_page; // 현재 페이지 업데이트
        renderPagination(data.pagination); // 페이지네이션 UI 렌더링
      } else {
        resultDiv.innerHTML = "<p>결과가 없거나 응답 형식이 올바르지 않습니다.</p>";
      }
    })
    .catch(err => {
      loadingDiv.style.display = "none";
      resultDiv.innerHTML = `<div class='error' style="color: red; text-align: center; padding: 10px;">보고서 생성 중 오류가 발생했습니다: ${err.message}</div>`;
      console.error("Report generation error:", err);
    });
  }

  // 폼 제출 시 첫 페이지 보고서 생성
  reportForm.addEventListener("submit", function(e) {
    e.preventDefault();
    currentPage = 1; // 폼 제출 시 항상 첫 페이지로 리셋
    generateReport(currentPage);
  }); //

  // 페이지네이션 UI 렌더링 함수
  function renderPagination(pagination) {
    paginationControlsDiv.innerHTML = ""; // 기존 UI 초기화

    if (!pagination || pagination.total_pages <= 1) {
      return; // 페이지가 하나거나 없으면 UI 생성 안 함
    }

    const { current_page, total_pages, items_per_page, total_items } = pagination;

    // 페이지 정보 텍스트 (예: 1 / 10 페이지 (총 150개 항목))
    const pageInfo = document.createElement("div");
    pageInfo.style.marginBottom = "10px";
    pageInfo.innerText = `페이지 ${current_page} / ${total_pages} (총 ${total_items}개 항목)`;
    paginationControlsDiv.appendChild(pageInfo);


    const buttonContainer = document.createElement("div");

    // 첫 페이지 버튼
    if (current_page > 1) {
        const firstButton = document.createElement("button");
        firstButton.innerText = "<< 처음";
        firstButton.addEventListener("click", () => generateReport(1));
        buttonContainer.appendChild(firstButton);
    }


    // 이전 버튼
    if (current_page > 1) {
      const prevButton = document.createElement("button");
      prevButton.innerText = "이전";
      prevButton.addEventListener("click", () => generateReport(current_page - 1));
      buttonContainer.appendChild(prevButton);
    }

    // 페이지 번호 버튼 (예: 현재 페이지 기준 앞뒤 2개씩, 총 5개 표시)
    const pageRange = 2; 
    let startPage = Math.max(1, current_page - pageRange);
    let endPage = Math.min(total_pages, current_page + pageRange);

    // 페이지네이션 시작 부분 ... 처리
    if (startPage > 1) {
        const pageButton = document.createElement("button");
        pageButton.innerText = "1";
        pageButton.addEventListener("click", () => generateReport(1));
        buttonContainer.appendChild(pageButton);
        if (startPage > 2) {
            const ellipsis = document.createElement("span");
            ellipsis.innerText = "...";
            ellipsis.style.margin = "0 5px";
            buttonContainer.appendChild(ellipsis);
        }
    }

    for (let i = startPage; i <= endPage; i++) {
      const pageButton = document.createElement("button");
      pageButton.innerText = i;
      if (i === current_page) {
        pageButton.disabled = true; 
        pageButton.style.fontWeight = 'bold';
        pageButton.style.textDecoration = 'underline';
      }
      pageButton.addEventListener("click", () => generateReport(i));
      buttonContainer.appendChild(pageButton);
    }

    // 페이지네이션 끝 부분 ... 처리
    if (endPage < total_pages) {
        if (endPage < total_pages - 1) {
            const ellipsis = document.createElement("span");
            ellipsis.innerText = "...";
            ellipsis.style.margin = "0 5px";
            buttonContainer.appendChild(ellipsis);
        }
        const pageButton = document.createElement("button");
        pageButton.innerText = total_pages;
        pageButton.addEventListener("click", () => generateReport(total_pages));
        buttonContainer.appendChild(pageButton);
    }

    // 다음 버튼
    if (current_page < total_pages) {
      const nextButton = document.createElement("button");
      nextButton.innerText = "다음";
      nextButton.addEventListener("click", () => generateReport(current_page + 1));
      buttonContainer.appendChild(nextButton);
    }

    // 마지막 페이지 버튼
    if (current_page < total_pages) {
        const lastButton = document.createElement("button");
        lastButton.innerText = "마지막 >>";
        lastButton.addEventListener("click", () => generateReport(total_pages));
        buttonContainer.appendChild(lastButton);
    }
    paginationControlsDiv.appendChild(buttonContainer);
  }
});
