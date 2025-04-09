async function generateReport() {
    const startDate = document.getElementById('start-date').value;
    const endDate = document.getElementById('end-date').value;
    
    if (!startDate || !endDate) {
        document.getElementById('error-message').textContent = '시작 날짜와 종료 날짜를 모두 입력해주세요.';
        return;
    }
    
    document.getElementById('error-message').textContent = '보고서를 생성 중입니다...';
    
    try {
        // 여기서 경로를 수정 - 앞에 슬래시를 추가하여 절대 경로로 변경
        const response = await fetch('/api/generate-report', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                start_date: startDate,
                end_date: endDate
            }),
        });
        
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const data = await response.json();
        
        if (data.error) {
            document.getElementById('error-message').textContent = `오류 발생: ${data.error}`;
            return;
        }
        
        document.getElementById('error-message').textContent = '';
        renderReport(data);
    } catch (error) {
        console.error("API 호출 오류:", error);
        document.getElementById('error-message').textContent = `오류 발생: ${error.message}`;
    }
}
