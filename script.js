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
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                start_date: startDate,
                end_date: endDate
            }),
        });
        
        const data = await response.json();
        
        if (data.error) {
            document.getElementById('error-message').textContent = `오류 발생: ${data.error}`;
            return;
        }
        
        document.getElementById('error-message').textContent = '';
        renderReport(data);
    } catch (error) {
        document.getElementById('error-message').textContent = `오류 발생: ${error.message}`;
    }
}

function renderReport(data) {
    const resultDiv = document.getElementById('report-result');
    
    let html = `
        <table>
            <tr>
                <th>광고명</th>
                <th>캠페인명</th>
                <th>광고세트명</th>
                <th>FB 광고비용</th>
                <th>노출</th>
                <th>Click</th>
                <th>CTR</th>
                <th>CPC</th>
                <th>광고 성과</th>
                <th>광고 이미지</th>
            </tr>
    `;
    
    data.forEach(row => {
        const rowClass = row['광고명'] === '합계' ? 'total-row' : '';
        let performanceClass = '';
        
        if (row['광고 성과'] === '고성과') {
            performanceClass = 'high-performance';
        } else if (row['광고 성과'] === '위닝콘텐츠') {
            performanceClass = 'winning-content';
        }
        
        const imgTag = row.image_url ? `<img src="${row.image_url}" alt="광고 이미지">` : '';
        
        html += `
            <tr class="${rowClass}">
                <td>${row['광고명'] || ''}</td>
                <td>${row['캠페인명'] || ''}</td>
                <td>${row['광고세트명'] || ''}</td>
                <td>${typeof row['FB 광고비용'] === 'number' ? row['FB 광고비용'].toFixed(2) : '0.00'}</td>
                <td>${typeof row['노출'] === 'number' ? row['노출'].toLocaleString() : '0'}</td>
                <td>${typeof row['Click'] === 'number' ? row['Click'].toLocaleString() : '0'}</td>
                <td>${row['CTR'] || '0%'}</td>
                <td>${typeof row['CPC'] === 'number' ? row['CPC'].toFixed(2) : '0.00'}</td>
                <td class="${performanceClass}">${row['광고 성과'] || ''}</td>
                <td>${imgTag}</td>
            </tr>
        `;
    });
    
    html += `</table>`;
    resultDiv.innerHTML = html;
}
