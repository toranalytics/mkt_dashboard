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

    const command = `python3 netlify/functions/get_facebook_ads_report.py ${startDate} ${endDate}`;
    
    return new Promise((resolve, reject) => {
      exec(command, (error, stdout, stderr) => {
        if (error) {
          console.error(`Error: ${stderr}`);
          reject({
            statusCode: 500,
            body: JSON.stringify({ error: "Failed to execute Python script" }),
          });
        } else {
          resolve({
            statusCode: 200,
            body: stdout,
            headers: { "Content-Type": "application/json" },
          });
        }
      });
    });
  } catch (err) {
    return {
      statusCode: 500,
      body: JSON.stringify({ error: "Internal server error" }),
    };
  }
};
