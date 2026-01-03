const nodemailer = require("nodemailer");
require("dotenv").config();

const transporter = nodemailer.createTransport({
  host: process.env.EMAIL_HOST,
  port: process.env.EMAIL_PORT,
  secure: false,
  auth: {
    user: process.env.EMAIL_USER,
    pass: process.env.EMAIL_PASS,
  },
});

async function sendRegistrationEmail({ email, password, user_name }) {
  await transporter.sendMail({
    from: `"SmartArena" <${process.env.EMAIL_USER}>`,
    to: email,
    subject: "Welcome to SmartArena",
    html: `
    <!DOCTYPE html>
    <html>
    <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <style>
      body {
        margin: 0;
        padding: 0;
        background-color: #f2f4f7;
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
      }
    
      .container {
        max-width: 600px;
        margin: 0 auto;
        background: #ffffff;
        border-radius: 12px;
        overflow: hidden;
      }
    
      .header {
        padding: 20px 24px;
        border-bottom: 1px solid #eaecf0;
      }
    
      .content {
        padding: 28px 24px;
      }
    
      .title {
        font-size: 22px;
        font-weight: 600;
        color: #101828;
        margin-bottom: 12px;
      }
    
      .text {
        font-size: 15px;
        line-height: 1.7;
        color: #475467;
        margin-bottom: 20px;
      }
    
      .card {
        background: #f9fafb;
        border: 1px solid #eaecf0;
        border-radius: 10px;
        padding: 16px;
        margin-bottom: 24px;
      }
    
      .card p {
        margin: 6px 0;
        font-size: 14px;
        color: #101828;
      }
    
      .label {
        font-size: 13px;
        color: #667085;
        margin-bottom: 8px;
      }
    
      .btn {
        display: inline-block;
        background: #2563eb;
        color: #ffffff;
        padding: 12px 20px;
        border-radius: 8px;
        text-decoration: none;
        font-size: 14px;
        font-weight: 500;
      }
    
      .footer {
        background: #f9fafb;
        padding: 16px 24px;
        font-size: 12px;
        color: #98a2b3;
      }
    
      /* Mobile Responsive */
      @media (max-width: 600px) {
        .container {
          border-radius: 0;
        }
    
        .title {
          font-size: 20px;
        }
    
        .text {
          font-size: 14px;
        }
    
        .content {
          padding: 22px 18px;
        }
      }
      .footer-dark {
        background-color: #0f172a; /* dark navy */
        padding: 20px 24px;
        text-align: center;
      }
    
      .footer-text {
        font-size: 12px;
        color: #cbd5e1; /* light gray */
        margin: 0;
        line-height: 1.6;
      }
    
      .footer-link {
        color: #93c5fd;
        text-decoration: none;
      }
    
      .footer-link:hover {
        text-decoration: underline;
      }
    
      @media (max-width: 600px) {
        .footer-dark {
          padding: 18px 16px;
        }
      }
      .header {
        padding: 20px 24px;
        border-bottom: 1px solid #eaecf0;
        background-color: #ffffff;
      }
    
    
    </style>
    </head>
    
    <body>
      <table width="100%" cellpadding="0" cellspacing="0">
        <tr>
          <td align="center" style="padding:24px 10px;">
    
            <table class="container" width="100%" cellpadding="0" cellspacing="0">
    
              <!-- Header -->
             <tr>
      <td class="header">
        <table width="100%" cellpadding="0" cellspacing="0">
          <tr>
            <td style="vertical-align: middle;">
              <table cellpadding="0" cellspacing="0">
                <tr>
                  <!-- Logo -->
                  <td style="vertical-align: middle;">
                    <img
                      src="https://res.cloudinary.com/dbaerswz1/image/upload/v1766481475/smartarena_nefngd.png"
                      alt="SmartArena"
                      style="height:38px; display:block;"
                    />
                  </td>
    
                  <!-- Brand Name -->
                  <td style="vertical-align: middle; padding-left:10px;">
                    <span style="
                      font-size:18px;
                      font-weight:600;
                      color:#101828;
                    ">
                      SmartArena
                    </span>
                  </td>
                </tr>
              </table>
            </td>
          </tr>
        </table>
      </td>
    </tr>
    
    
              <!-- Content -->
              <tr>
                <td class="content">
                  <div class="title">Welcome ${user_name || ""}</div>
    
                  <div class="text">
                    Your SmartArena account has been successfully created.
                    You can sign in using the credentials below.
                  </div>
    
                  <div class="card">
                    <div class="label">Login details</div>
                    <p><strong>Email:</strong> ${email}</p>
                    <p><strong>Password:</strong> ${password}</p>
                  </div>
    
                  <a href="https://your-app-domain.com/login" class="btn">
                    Sign in to SmartArena
                  </a>
    
                  <div class="text" style="margin-top:20px; font-size:13px;">
                    For security reasons, please change your password after your first login.
                  </div>
    
                  <div class="text" style="margin-top:28px;">
                    Thanks,<br />
                    <strong>SmartArena Team</strong>
                  </div>
                </td>
              </tr>
    
              <!-- Footer -->
              <tr>
      <td class="footer-dark">
        <p class="footer-text">
          © ${new Date().getFullYear()} SmartArena. All rights reserved.
        </p>
        <p class="footer-text">
          Need help?
          <a href="mailto:support@smartarena.com" class="footer-link">
            support@smartarena.com
          </a>
        </p>
      </td>
    </tr>
    
    
            </table>
    
          </td>
        </tr>
      </table>
    </body>
    </html>
`,
  });
}

module.exports = { sendRegistrationEmail };
