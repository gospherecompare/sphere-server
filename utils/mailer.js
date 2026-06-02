const nodemailer = require("nodemailer");
require("dotenv").config();

const EMAIL_PORT = Number(process.env.EMAIL_PORT || 587);
const EMAIL_SECURE =
  process.env.EMAIL_SECURE === "true" ? true : EMAIL_PORT === 465;

const buildFromHeader = ({ name, email, fallback }) => {
  const safeName = String(name || "").trim();
  const safeEmail = String(email || "").trim();

  if (safeName && safeEmail) {
    return `${safeName} <${safeEmail}>`;
  }

  if (safeEmail) {
    return safeEmail;
  }

  if (safeName) {
    return safeName;
  }

  return fallback;
};

const EMAIL_FROM =
  process.env.EMAIL_FROM ||
  buildFromHeader({
    name: process.env.EMAIL_FROM_NAME || "SmartArena",
    email: process.env.EMAIL_FROM_ADDRESS || process.env.EMAIL_USER,
    fallback: "SmartArena <no-reply@example.com>",
  });

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
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background:yellow;">
      <tbody><tr>
        <td align="center">
  
          <table role="presentation" class="wrapper" width="620" cellpadding="0" cellspacing="0" border="0" style="width:100%;">
  
            <tbody><tr>
              <td class="header-cell" style="background:linear-gradient(160deg,#1c1136 0%,#130e2a 100%);padding:26px 40px;border-bottom:none;">
                <span style="display:inline-block;width:32px;height:32px;background:linear-gradient(135deg,#7b5ef8 0%,#5b8af7 100%);border-radius:7px;text-align:center;vertical-align:middle;margin-right:9px;line-height:32px;">
                  <span style="display:inline-block;vertical-align:middle;">
                    <span style="display:inline-block;width:5px;height:12px; margin-top:-3px; background:#fff;margin-right:1px;vertical-align:middle;"></span>
                    <span style="display:inline-block;width:5px;height:12px;margin-top:-3px;background:#fff;vertical-align:middle;"></span>
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

const formatMultilineText = (value) => {
  const safe = escapeHtml(value || "");
  if (!safe) return "";
  return safe.replace(/\n/g, "<br />");
};

const formatDateTimeLabel = (value, timeZone) => {
  if (!value) return "";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return escapeHtml(String(value));
  }
  const options = {
    year: "numeric",
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  };
  let formatted;
  try {
    formatted = parsed.toLocaleString("en-IN", {
      ...options,
      timeZone: timeZone || undefined,
    });
  } catch (err) {
    formatted = parsed.toLocaleString("en-IN", options);
  }
  return timeZone ? `${formatted} (${escapeHtml(timeZone)})` : formatted;
};

const getFileNameFromUrl = (url, fallback) => {
  if (!url) return fallback;
  try {
    const parsed = new URL(url);
    const name = parsed.pathname.split("/").filter(Boolean).pop();
    return name || fallback;
  } catch (err) {
    return fallback;
  }
};

const renderCareerEmailShell = ({ title, bodyHtml }) => `
<!DOCTYPE html>
<html>
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>${escapeHtml(title || "Hooks Careers")}</title>
  </head>
  <body style="margin:0;padding:0;background:#ffffff;">
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background:#ffffff;padding:28px 12px 48px;">
      <tbody>
        <tr>
          <td align="center">
            <table role="presentation" width="620" cellpadding="0" cellspacing="0" border="0" style="max-width:620px;width:100%;">
              <tbody>
                <tr>
                  <td style="background:linear-gradient(160deg,#1c1136 0%,#130e2a 100%);padding:26px 40px;border:1px solid rgba(123,94,248,0.22);border-bottom:none;">
                    <span style="display:inline-block;width:32px;height:32px;background:linear-gradient(135deg,#7b5ef8 0%,#5b8af7 100%);border-radius:7px;text-align:center;vertical-align:middle;margin-right:9px;line-height:32px;">
                      <span style="display:inline-block;vertical-align:middle;">
                        <span style="display:inline-block;width:5px;height:12px;background:#fff;margin-right:1px;vertical-align:middle;"></span>
                        <span style="display:inline-block;width:5px;height:12px;background:#fff;vertical-align:middle;"></span>
                      </span>
                    </span>
                    <span style="font-family:'Syne',sans-serif;font-size:18px;font-weight:800;color:#ffffff;letter-spacing:-0.4px;vertical-align:middle;">Hooks</span>
                    <span style="font-size:10px;font-weight:500;color:#5a4d80;letter-spacing:2.5px;text-transform:uppercase;margin-left:9px;vertical-align:middle;">Careers</span>
                  </td>
                </tr>
                <tr>
                  <td style="height:2px;background:linear-gradient(90deg,#7b5ef8 0%,#5b8af7 55%,rgba(91,138,247,0.05) 100%);border-left:1px solid rgba(123,94,248,0.2);border-right:1px solid rgba(123,94,248,0.2);"></td>
                </tr>
                <tr>
                  <td style="background:#ffffff;padding:42px 40px;border-left:1px solid rgba(123,94,248,0.12);border-right:1px solid rgba(123,94,248,0.12);">
                    ${bodyHtml || ""}
                  </td>
                </tr>
                <tr>
                  <td style="background:#000000;border:1px solid rgba(123,94,248,0.2);border-top:none;padding:20px 40px;">
                    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
                      <tbody>
                        <tr>
                          <td style="vertical-align:middle;">
                            <p style="margin:0;font-size:11px;color:#f0f0f0;line-height:1.5;">
                              &copy; 2026
                              <a href="https://tryhook.shop/" style="color:#8b72ff;text-decoration:none;font-weight:600;">tryhook.shop</a>
                              &nbsp;&middot;&nbsp; All rights reserved.
                            </p>
                          </td>
                          <td align="right" style="vertical-align:middle;">
                            <a href="https://www.linkedin.com/company/hooks-data/?viewAsMember=true" style="display:inline-block;width:30px;height:30px;background:rgba(123,94,248,0.22);border-radius:7px;text-align:center;line-height:30px;color:#7b5ef8;text-decoration:none;font-size:10px;font-weight:700;">in</a>
                          </td>
                        </tr>
                      </tbody>
                    </table>
                  </td>
                </tr>
              </tbody>
            </table>
          </td>
        </tr>
      </tbody>
    </table>
  </body>
</html>
`;

const renderCareerAssignmentTemplate = ({
  role,
  firstName,
  lastName,
  message,
  pdfUrl,
  dueDateLabel,
}) => {
  const safeRole = escapeHtml(role || "");
  const safeName = escapeHtml(
    `${firstName || ""} ${lastName || ""}`.trim() || "there",
  );
  const safeMessage = formatMultilineText(message);
  const dueLine = dueDateLabel
    ? `<p style="margin:0 0 14px;font-size:14px;line-height:1.7;color:#3d3550;">Assignment due date: <strong>${dueDateLabel}</strong></p>`
    : "";
  const assignmentLink = pdfUrl
    ? `<p style="margin:0 0 18px;font-size:14px;line-height:1.7;color:#3d3550;">
        You can access the assignment PDF here:
        <a href="${escapeHtml(pdfUrl)}" style="color:#5b3ef8;text-decoration:none;font-weight:600;">Download assignment</a>
      </p>`
    : "";

  return renderCareerEmailShell({
    title: "Assignment from Hooks",
    bodyHtml: `
      <p style="margin:0 0 10px;font-size:15px;line-height:1.8;color:#3d3550;">
        Dear <strong style="color:#0e0b1a;">${safeName}</strong>,
      </p>
      <p style="margin:0 0 16px;font-size:15px;line-height:1.8;color:#3d3550;">
        We are excited to move you forward for ${
          safeRole || "the role you applied for"
        }. Please complete the assignment shared below.
      </p>
      ${dueLine}
      ${assignmentLink}
      ${
        safeMessage
          ? `<p style="margin:0 0 18px;font-size:14px;line-height:1.7;color:#3d3550;">${safeMessage}</p>`
          : ""
      }
      <p style="margin:0;font-size:14px;line-height:1.7;color:#3d3550;">
        If you have any questions, reply to this email and we will help.
      </p>
      <table role="presentation" cellpadding="0" cellspacing="0" border="0" style="margin-top:30px;padding-top:18px;">
        <tbody>
          <tr>
            <td style="vertical-align:middle;padding-right:16px;">
              <p style="margin:0 0 2px;font-family:'Syne',sans-serif;font-size:14px;font-weight:700;color:#0e0b1a;">Hiring Hooks</p>
              <p style="margin:0;font-size:11px;color:#8b72ff;font-weight:400;">Hooks</p>
            </td>
          </tr>
        </tbody>
      </table>
    `,
  });
};

const renderCareerInterviewTemplate = ({
  role,
  firstName,
  lastName,
  message,
  meetLink,
  scheduleLabel,
}) => {
  const safeRole = escapeHtml(role || "");
  const safeName = escapeHtml(
    `${firstName || ""} ${lastName || ""}`.trim() || "there",
  );
  const safeMessage = formatMultilineText(message);
  const meetLine = meetLink
    ? `<p style="margin:0 0 12px;font-size:14px;line-height:1.7;color:#3d3550;">
        Google Meet link:
        <a href="${escapeHtml(meetLink)}" style="color:#5b3ef8;text-decoration:none;font-weight:600;">Join interview</a>
      </p>`
    : "";
  const scheduleLine = scheduleLabel
    ? `<p style="margin:0 0 12px;font-size:14px;line-height:1.7;color:#3d3550;">
        Scheduled for: <strong>${scheduleLabel}</strong>
      </p>`
    : "";

  return renderCareerEmailShell({
    title: "Interview scheduled",
    bodyHtml: `
      <p style="margin:0 0 10px;font-size:15px;line-height:1.8;color:#3d3550;">
        Dear <strong style="color:#0e0b1a;">${safeName}</strong>,
      </p>
      <p style="margin:0 0 16px;font-size:15px;line-height:1.8;color:#3d3550;">
        Your interview for ${safeRole || "the role you applied for"} has been scheduled.
      </p>
      ${scheduleLine}
      ${meetLine}
      ${
        safeMessage
          ? `<p style="margin:0 0 18px;font-size:14px;line-height:1.7;color:#3d3550;">${safeMessage}</p>`
          : ""
      }
      <p style="margin:0;font-size:14px;line-height:1.7;color:#3d3550;">
        If you need to reschedule, please reply to this email.
      </p>
      <table role="presentation" cellpadding="0" cellspacing="0" border="0" style="margin-top:30px;padding-top:18px;">
        <tbody>
          <tr>
            <td style="vertical-align:middle;padding-right:16px;">
              <p style="margin:0 0 2px;font-family:'Syne',sans-serif;font-size:14px;font-weight:700;color:#0e0b1a;">Hiring Hooks</p>
              <p style="margin:0;font-size:11px;color:#8b72ff;font-weight:400;">Hooks</p>
            </td>
          </tr>
        </tbody>
      </table>
    `,
  });
};

const renderCareerHrTemplate = ({
  role,
  firstName,
  lastName,
  message,
  scheduleLabel,
}) => {
  const safeRole = escapeHtml(role || "");
  const safeName = escapeHtml(
    `${firstName || ""} ${lastName || ""}`.trim() || "there",
  );
  const safeMessage = formatMultilineText(message);
  const scheduleLine = scheduleLabel
    ? `<p style="margin:0 0 12px;font-size:14px;line-height:1.7;color:#3d3550;">
        HR round scheduled for: <strong>${scheduleLabel}</strong>
      </p>`
    : "";

  return renderCareerEmailShell({
    title: "HR round update",
    bodyHtml: `
      <p style="margin:0 0 10px;font-size:15px;line-height:1.8;color:#3d3550;">
        Dear <strong style="color:#0e0b1a;">${safeName}</strong>,
      </p>
      <p style="margin:0 0 16px;font-size:15px;line-height:1.8;color:#3d3550;">
        We are moving you forward to the HR round for ${
          safeRole || "the role you applied for"
        }.
      </p>
      ${scheduleLine}
      ${
        safeMessage
          ? `<p style="margin:0 0 18px;font-size:14px;line-height:1.7;color:#3d3550;">${safeMessage}</p>`
          : ""
      }
      <p style="margin:0;font-size:14px;line-height:1.7;color:#3d3550;">
        Please reply if you have any availability constraints.
      </p>
      <table role="presentation" cellpadding="0" cellspacing="0" border="0" style="margin-top:30px;padding-top:18px;">
        <tbody>
          <tr>
            <td style="vertical-align:middle;padding-right:16px;">
              <p style="margin:0 0 2px;font-family:'Syne',sans-serif;font-size:14px;font-weight:700;color:#0e0b1a;">Hiring Hooks</p>
              <p style="margin:0;font-size:11px;color:#8b72ff;font-weight:400;">Hooks</p>
            </td>
          </tr>
        </tbody>
      </table>
    `,
  });
};

const renderCareerOfferTemplate = ({
  role,
  firstName,
  lastName,
  message,
  offerUrl,
}) => {
  const safeRole = escapeHtml(role || "");
  const safeName = escapeHtml(
    `${firstName || ""} ${lastName || ""}`.trim() || "there",
  );
  const safeMessage = formatMultilineText(message);
  const offerLink = offerUrl
    ? `<p style="margin:0 0 16px;font-size:14px;line-height:1.7;color:#3d3550;">
        Offer document:
        <a href="${escapeHtml(offerUrl)}" style="color:#5b3ef8;text-decoration:none;font-weight:600;">View offer</a>
      </p>`
    : "";

  return renderCareerEmailShell({
    title: "Offer from Hooks",
    bodyHtml: `
      <p style="margin:0 0 10px;font-size:15px;line-height:1.8;color:#3d3550;">
        Dear <strong style="color:#0e0b1a;">${safeName}</strong>,
      </p>
      <p style="margin:0 0 16px;font-size:15px;line-height:1.8;color:#3d3550;">
        We are pleased to share the offer details for ${
          safeRole || "the role you applied for"
        }.
      </p>
      ${offerLink}
      ${
        safeMessage
          ? `<p style="margin:0 0 18px;font-size:14px;line-height:1.7;color:#3d3550;">${safeMessage}</p>`
          : ""
      }
      <p style="margin:0;font-size:14px;line-height:1.7;color:#3d3550;">
        Please reply to this email with any questions.
      </p>
      <table role="presentation" cellpadding="0" cellspacing="0" border="0" style="margin-top:30px;padding-top:18px;">
        <tbody>
          <tr>
            <td style="vertical-align:middle;padding-right:16px;">
              <p style="margin:0 0 2px;font-family:'Syne',sans-serif;font-size:14px;font-weight:700;color:#0e0b1a;">Hiring Hooks</p>
              <p style="margin:0;font-size:11px;color:#8b72ff;font-weight:400;">Hooks</p>
            </td>
          </tr>
        </tbody>
      </table>
    `,
  });
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

async function sendCareerAssignmentEmail({
  email,
  role,
  firstName,
  lastName,
  subject,
  message,
  pdfUrl,
  dueDateLabel,
}) {
  const mailSubject =
    subject || `Assignment details${role ? ` - ${role}` : ""}`;
  const text = `Please complete the assignment for ${
    role || "the role you applied for"
  }.${dueDateLabel ? ` Due date: ${dueDateLabel}.` : ""} ${
    pdfUrl ? `Assignment: ${pdfUrl}` : ""
  }`;

  await transporter.sendMail({
    from: EMAIL_FROM,
    to: email,
    subject: mailSubject,
    text,
    html: renderCareerAssignmentTemplate({
      role,
      firstName,
      lastName,
      message,
      pdfUrl,
      dueDateLabel,
    }),
    attachments: pdfUrl
      ? [
          {
            filename: getFileNameFromUrl(pdfUrl, "assignment.pdf"),
            path: pdfUrl,
          },
        ]
      : undefined,
  });
}

async function sendCareerInterviewEmail({
  email,
  role,
  firstName,
  lastName,
  subject,
  message,
  meetLink,
  scheduledAt,
  timeZone,
}) {
  const mailSubject =
    subject || `Interview scheduled${role ? ` - ${role}` : ""}`;
  const scheduleLabel = scheduledAt
    ? formatDateTimeLabel(scheduledAt, timeZone)
    : "";
  const text = `Your interview for ${
    role || "the role you applied for"
  } has been scheduled.${scheduleLabel ? ` ${scheduleLabel}.` : ""} ${
    meetLink ? `Meeting link: ${meetLink}` : ""
  }`;

  await transporter.sendMail({
    from: EMAIL_FROM,
    to: email,
    subject: mailSubject,
    text,
    html: renderCareerInterviewTemplate({
      role,
      firstName,
      lastName,
      message,
      meetLink,
      scheduleLabel,
    }),
  });
}

async function sendCareerHrEmail({
  email,
  role,
  firstName,
  lastName,
  subject,
  message,
  scheduledAt,
  timeZone,
}) {
  const mailSubject = subject || `HR round update${role ? ` - ${role}` : ""}`;
  const scheduleLabel = scheduledAt
    ? formatDateTimeLabel(scheduledAt, timeZone)
    : "";
  const text = `We are moving you to the HR round for ${
    role || "the role you applied for"
  }.${scheduleLabel ? ` Scheduled: ${scheduleLabel}.` : ""}`;

  await transporter.sendMail({
    from: EMAIL_FROM,
    to: email,
    subject: mailSubject,
    text,
    html: renderCareerHrTemplate({
      role,
      firstName,
      lastName,
      message,
      scheduleLabel,
    }),
  });
}

async function sendCareerOfferEmail({
  email,
  role,
  firstName,
  lastName,
  subject,
  message,
  offerUrl,
}) {
  const mailSubject = subject || `Offer details${role ? ` - ${role}` : ""}`;
  const text = `We are pleased to share the offer details for ${
    role || "the role you applied for"
  }.${offerUrl ? ` Offer: ${offerUrl}` : ""}`;

  await transporter.sendMail({
    from: EMAIL_FROM,
    to: email,
    subject: mailSubject,
    text,
    html: renderCareerOfferTemplate({
      role,
      firstName,
      lastName,
      message,
      offerUrl,
    }),
    attachments: offerUrl
      ? [
          {
            filename: getFileNameFromUrl(offerUrl, "offer.pdf"),
            path: offerUrl,
          },
        ]
      : undefined,
  });
}

async function sendLoginOtpEmail({
  email,
  otp,
  userName,
  expiresInMinutes = 5,
}) {
  const safeName = escapeHtml(userName || "there");
  const safeOtp = escapeHtml(otp);
  const safeMinutes = Number.isFinite(expiresInMinutes)
    ? Math.max(1, Math.floor(expiresInMinutes))
    : 5;

  await transporter.sendMail({
    from: EMAIL_FROM,
    to: email,
    subject: "Your Hook login verification code",
    text: `Hi ${userName || "there"}, your login code is ${otp}. It expires in ${safeMinutes} minutes. If you did not request this, ignore this message.`,
    html: `
      <!DOCTYPE html>
      <html>
      <head>
        <meta charset="UTF-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1.0" />
      </head>
      <body style="margin:0;padding:0;background:#f4f7fb;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
        <div style="max-width:600px;margin:0 auto;padding:32px 16px;">
          <div style="background:#ffffff;border-radius:16px;overflow:hidden;border:1px solid #e5e7eb;">
            <div style="padding:24px 28px;border-bottom:1px solid #eef2f7;">
              <div style="font-size:13px;letter-spacing:0.08em;text-transform:uppercase;color:#6b7280;margin-bottom:8px;">Hook login security</div>
              <div style="font-size:24px;font-weight:700;color:#111827;line-height:1.25;">Your verification code</div>
            </div>
            <div style="padding:28px;">
              <p style="margin:0 0 16px;color:#374151;font-size:15px;line-height:1.6;">Hi ${safeName},</p>
              <p style="margin:0 0 20px;color:#374151;font-size:15px;line-height:1.6;">
                Use the code below to finish signing in. It will expire in ${safeMinutes} minutes.
              </p>
              <div style="display:inline-block;background:#f9fafb;border:1px solid #d1d5db;border-radius:14px;padding:18px 24px;font-size:32px;font-weight:800;letter-spacing:0.2em;color:#111827;">
                ${safeOtp}
              </div>
              <p style="margin:20px 0 0;color:#6b7280;font-size:13px;line-height:1.6;">
                If you did not request this code, you can ignore this email. Your password and account remain unchanged.
              </p>
            </div>
          </div>
        </div>
      </body>
      </html>
    `,
  });
}

module.exports = {
  sendRegistrationEmail,
  sendRegistrationMail: sendRegistrationEmail,
  sendLoginOtpEmail,
  sendCareerApplicationEmail,
  sendCareerAssignmentEmail,
  sendCareerInterviewEmail,
  sendCareerHrEmail,
  sendCareerOfferEmail,
};
