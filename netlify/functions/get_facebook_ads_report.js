const { exec } = require("child_process");
const path = require("path");

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

    // 절대 경로 사용
    const scriptPath = path.join(__dirname, "get_facebook_ads_report.py");
    const command = `python3 ${scriptPath} ${startDate} ${endDate}`;
    
    return new Promise((resolve, reject) => {
      exec(command, { env: process.env }, (error, stdout, stderr) => {
        if (error) {
          console.error(`Error: ${stderr}`);
          return resolve({
            statusCode: 500,
            body: JSON.stringify({ error: stderr || "Script execution failed" }),
          });
        }
        
        try {
          const jsonOutput = JSON.parse(stdout);
          return resolve({
            statusCode: 200,
            body: JSON.stringify(jsonOutput),
            headers: { "Content-Type": "application/json" },
          });
        } catch (parseError) {
          console.error(`Failed to parse output: ${stdout}`);
          return resolve({
            statusCode: 500,
            body: JSON.stringify({ error: "Invalid JSON output", stdout: stdout }),
          });
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
