const nodemailer = require("nodemailer");
require("dotenv").config();

const EMAIL_PORT = Number(process.env.EMAIL_PORT || 587);
const EMAIL_SECURE =
  process.env.EMAIL_SECURE === "true" ? true : EMAIL_PORT === 465;
const EMAIL_FROM =
  process.env.EMAIL_FROM ||
  (process.env.EMAIL_USER
    ? `SmartArena <${process.env.EMAIL_USER}>`
    : "SmartArena <no-reply@example.com>");

const transporter = nodemailer.createTransport({
  host: process.env.EMAIL_HOST,
  port: EMAIL_PORT,
  secure: EMAIL_SECURE,
  auth: {
    user: process.env.EMAIL_USER,
    pass: process.env.EMAIL_PASS,
  },
});

const escapeHtml = (value) =>
  String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");

async function sendRegistrationEmail({ email, password, user_name }) {
  await transporter.sendMail({
    from: EMAIL_FROM,
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

const renderCareerApplicationTemplate = ({ role, firstName, lastName }) => {
  const safeRole = escapeHtml(role || "");
  const roleLabel = safeRole || "the role you applied for";
  const safeName = escapeHtml(
    `${firstName || ""} ${lastName || ""}`.trim() || "there",
  );

  return `<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Application Received</title>
</head>
<body style="margin:0;padding:0;background:#ffffff;">
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background:#ffffff;padding:40px 12px 60px;">
    <tbody><tr>
      <td align="center">

        <table role="presentation" class="wrapper" width="620" cellpadding="0" cellspacing="0" border="0" style="max-width:620px;width:100%;">

          <tbody><tr>
            <td class="header-cell" style="background:linear-gradient(160deg,#1c1136 0%,#130e2a 100%);padding:26px 40px;border:1px solid rgba(123,94,248,0.22);border-bottom:none;">
              <span style="display:inline-block;width:32px;height:32px;background:linear-gradient(135deg,#7b5ef8 0%,#5b8af7 100%);border-radius:7px;text-align:center;vertical-align:middle;margin-right:9px;line-height:32px;">
                <span style="display:inline-block;vertical-align:middle;">
                  <span style="display:inline-block;width:5px;height:12px;background:#fff;margin-right:1px;vertical-align:middle;"></span>
                  <span style="display:inline-block;width:5px;height:12px;background:#fff;vertical-align:middle;"></span>
                </span>
              </span>
              <span class="logo-text" style="font-family:'Syne',sans-serif;font-size:18px;font-weight:800;color:#ffffff;letter-spacing:-0.4px;vertical-align:middle;">Hooks</span>
              <span style="font-size:10px;font-weight:500;color:#5a4d80;letter-spacing:2.5px;text-transform:uppercase;margin-left:9px;vertical-align:middle;">Careers</span>
            </td>
          </tr>

          <tr>
            <td style="height:2px;background:linear-gradient(90deg,#7b5ef8 0%,#5b8af7 55%,rgba(91,138,247,0.05) 100%);border-left:1px solid rgba(123,94,248,0.2);border-right:1px solid rgba(123,94,248,0.2);"></td>
          </tr>

          <tr>
            <td class="body-cell" style="background:#ffffff;padding:46px 40px 38px;border-left:1px solid rgba(123,94,248,0.12);border-right:1px solid rgba(123,94,248,0.12);">

              <p style="margin:0 0 5px;font-size:10px;color:#8b72ff;letter-spacing:3px;text-transform:uppercase;font-weight:600;">We've got you</p>

              <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-bottom:22px;">
                <tbody><tr><td style="height:1px;background:linear-gradient(90deg,#ede9ff 0%,rgba(255,255,255,0) 100%);"></td></tr>
              </tbody></table>

              <p class="body-copy" style="margin:0 0 10px;font-size:15px;line-height:1.85;color:#3d3550;font-weight:300;">
                Dear <strong style="font-weight:600;color:#0e0b1a;">${safeName}</strong>,
              </p>
              <p class="body-copy" style="margin:0 0 16px;font-size:15px;line-height:1.85;color:#3d3550;font-weight:300;">
                We've successfully received your application for the
                <strong style="font-weight:600;color:#5b3ef8;">${roleLabel}</strong>
                at Hooks. Our recruitment team has been notified and your profile is now under active review.
              </p>
              <p class="body-copy" style="margin:0 0 16px;font-size:15px;line-height:1.85;color:#3d3550;font-weight:300;">
                Our hiring team will carefully evaluate your qualifications and experience against the requirements of the role. If your profile is a strong match, we'll reach out to discuss next steps.
              </p>
              <p class="body-copy" style="margin:0;font-size:15px;line-height:1.85;color:#3d3550;font-weight:300;">We truly appreciate your interest in joining Hooks.</p>

              <table role="presentation" cellpadding="0" cellspacing="0" border="0" style="margin-top:34px;padding-top:22px;">
                <tbody><tr>
                  <td style="vertical-align:middle;padding-right:16px;">
                    <p class="sig-name" style="margin:0 0 2px;font-family:'Syne',sans-serif;font-size:14px;font-weight:700;color:#0e0b1a;">Hiring Hooks</p>
                    <p style="margin:0;font-size:11px;color:#8b72ff;font-weight:400;">Hooks</p>
                  </td>
                </tr>
              </tbody></table>

            </td>
          </tr>

          <tr>
            <td class="footer-cell" style="background:#000000;border:1px solid rgba(123,94,248,0.2);border-top:none;padding:20px 40px;">
              <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
                <tbody><tr>
                  <td style="vertical-align:middle;">
                    <p class="footer-copy" style="margin:0;font-size:11px;color:#f0f0f0;line-height:1.5;">
                      &copy; 2026
                      <a href="https://tryhook.shop/" style="color:#8b72ff;text-decoration:none;font-weight:600;">tryhook.shop</a>
                      &nbsp;&middot;&nbsp; All rights reserved.
                    </p>
                  </td>
                  <td align="right" style="vertical-align:middle;">
                    <a href="https://www.linkedin.com/company/hooks-data/?viewAsMember=true" class="social-icon" style="display:inline-block;width:30px;height:30px;background:rgba(123,94,248,0.22);border-radius:7px;text-align:center;line-height:30px;color:#7b5ef8;text-decoration:none;font-size:10px;font-weight:700;">in</a>
                  </td>
                </tr>
              </tbody></table>
            </td>
          </tr>

        </tbody></table>

      </td>
    </tr>
  </tbody></table>
</body>
</html>`;
};

async function sendCareerApplicationEmail({
  email,
  role,
  firstName,
  lastName,
}) {
  const subject = `Application received${role ? ` - ${role}` : ""}`;
  const text = `We have successfully received your application${
    role ? ` for ${role}` : ""
  }. Our hiring team will review your profile and get back to you if there is a match.`;

  await transporter.sendMail({
    from: EMAIL_FROM,
    to: email,
    subject,
    text,
    html: renderCareerApplicationTemplate({ role, firstName, lastName }),
  });
}

module.exports = {
  sendRegistrationEmail,
  sendRegistrationMail: sendRegistrationEmail,
  sendCareerApplicationEmail,
};
