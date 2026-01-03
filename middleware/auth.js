const jwt = require("jsonwebtoken");
const SECRET = process.env.JWT_SECRET;

function authenticateCustomer(req, res, next) {
  const authHeader = req.headers.authorization;
  const token = authHeader && authHeader.split(" ")[1];

  if (!token) {
    return res.status(401).json({ message: "Customer token required" });
  }

  try {
    const decoded = jwt.verify(token, SECRET);

    // 🔒 ensure token belongs to customer
    if (decoded.type !== "customer") {
      return res.status(403).json({ message: "Invalid customer token" });
    }

    req.customer = decoded;
    next();
  } catch (err) {
    return res.status(401).json({ message: "Invalid or expired token" });
  }
}

function authenticate(req, res, next) {
  const authHeader = req.headers.authorization;
  const token = authHeader && authHeader.split(" ")[1];
  if (!token) return res.status(401).json({ message: "No token provided" });
  try {
    const decoded = jwt.verify(token, SECRET);
    req.user = decoded;
    next();
  } catch (err) {
    return res.status(403).json({ message: "Invalid token" });
  }
}

module.exports = { authenticateCustomer, authenticate };
