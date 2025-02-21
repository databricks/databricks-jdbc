#!/bin/bash

# Create the HTML output file
cat > jdbc-comparison-report.html << 'EOL'
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <style>
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            line-height: 1.6;
            color: #333;
            max-width: 1000px;
            margin: 0 auto;
            padding: 20px;
        }
        .header {
            border-bottom: 2px solid #0062cc;
            padding-bottom: 10px;
            margin-bottom: 20px;
        }
        .greeting {
            font-size: 16px;
            margin-bottom: 20px;
        }
        .comparator-section {
            background-color: #f8f9fa;
            border: 1px solid #e9ecef;
            border-radius: 4px;
            padding: 15px;
            margin: 20px 0;
        }
        .section-title {
            background-color: #0062cc;
            color: white;
            padding: 8px 15px;
            border-radius: 4px;
            margin: -15px -15px 15px -15px;
            font-weight: bold;
        }
        .driver-info {
            background-color: #e9ecef;
            border-radius: 4px;
            padding: 10px 15px;
            margin-bottom: 15px;
            font-size: 14px;
        }
        .driver-info code {
            background-color: #fff;
            padding: 2px 5px;
            border-radius: 3px;
            font-family: 'Courier New', Courier, monospace;
        }
        .output {
            font-family: 'Courier New', Courier, monospace;
            white-space: pre-wrap;
            background-color: #fff;
            border: 1px solid #dee2e6;
            border-radius: 3px;
            padding: 10px;
            margin: 10px 0;
        }
        .vs-highlight {
            font-weight: bold;
            color: #0062cc;
            background-color: #e6f3ff;
            padding: 2px 6px;
            border-radius: 3px;
            margin: 0 3px;
        }
        .notes {
            background-color: #fff3cd;
            border: 1px solid #ffeeba;
            border-radius: 4px;
            padding: 15px;
            margin: 20px 0;
        }
        .notes h3 {
            color: #856404;
            margin-top: 0;
        }
        .notes ul {
            margin: 10px 0;
            padding-left: 20px;
        }
        .footer {
            margin-top: 30px;
            padding-top: 15px;
            border-top: 1px solid #dee2e6;
            font-size: 14px;
            color: #6c757d;
        }
    </style>
</head>
<body>
    <div class="header">
        <h2>JDBC Driver Comparison Results</h2>
    </div>

    <div class="greeting">
        Hi Team,
    </div>

    <p>Please find the JDBC driver comparison results below. These differences were detected during our automated comparison test run on prod e2-benchfood.</p>

    <div class="comparator-section">
        <div class="section-title">Comparator Output</div>
        <div class="driver-info">
            In the comparison results below:
            <ul>
                <li>Values before <span class="vs-highlight">vs</span> are from <code>Old</code> driver</li>
                <li>Values after <span class="vs-highlight">vs</span> are from <code>OSS</code> driver</li>
            </ul>
        </div>
        <div class="output" id="comparison-output">
EOL

# Escape HTML special characters, highlight 'vs', and append the comparison output
cat jdbc-comparison-report.txt | sed 's/&/\&amp;/g; s/</\&lt;/g; s/>/\&gt;/g; s/ vs /<span class="vs-highlight">vs<\/span>/g' >> jdbc-comparison-report.html

# Append the rest of the HTML template
cat >> jdbc-comparison-report.html << 'EOL'
        </div>
    </div>

    <div class="notes">
        <h3>Important Notes</h3>
        <ul>
            <li>If you believe any of these differences are acceptable and should be excluded from future reports, please contact <a href="mailto:jayant.singh@databricks.com">jayant.singh@databricks.com</a> with your reasoning and approval.</li>
            <li>To suggest additional metadata methods or SQL queries for comparison, please reach out to <a href="mailto:jayant.singh@databricks.com">jayant.singh@databricks.com</a>.</li>
        </ul>
    </div>

    <div class="footer">
        Thanks!<br>
        JDBC Comparator Runner
    </div>
</body>
</html>
EOL