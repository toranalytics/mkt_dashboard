const { exec } = require("child_process");

exports.handler = async (event) => {
  try {
    const body = JSON.parse(event.body);
    const startDate = body.start_date;
    const endDate = body.end_date;

    if (!startDate || !endDate) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "Missing start_date or end_date" }),
      };
    }

    const command = `python3 ${__dirname}/get_facebook_ads_report.py ${startDate} ${endDate}`;
    
    return new Promise((resolve, reject) => {
      exec(command, (error, stdout, stderr) => {
        if (error) {
          console.error(`Error: ${stderr}`);
          reject({
            statusCode: 500,
            body: JSON.stringify({ error: stderr }),
          });
        } else {
          try {
            const jsonOutput = JSON.parse(stdout); // Python 출력이 JSON 형식인지 확인
            resolve({
              statusCode: 200,
              body: JSON.stringify(jsonOutput),
              headers: { "Content-Type": "application/json" },
            });
          } catch (parseError) {
            console.error(`Failed to parse Python output: ${stdout}`);
            reject({
              statusCode: 500,
              body: JSON.stringify({ error: "Invalid JSON output from Python script" }),
            });
          }
        }
      });
    });
  } catch (err) {
    console.error(`Unexpected error: ${err}`);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: "Internal server error" }),
    };
  }
};
