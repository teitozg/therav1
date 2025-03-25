import React from "react";
import { useAuth } from "../hooks/useAuth";
import "./AuthPage.css";

function AuthPage({ onLogin }) {
  const {
    email,
    setEmail,
    otp,
    setOtp,
    step,
    loading,
    error,
    sendOTP,
    verifyOTP,
  } = useAuth();

  const handleEmailSubmit = async (e) => {
    e.preventDefault();
    await sendOTP(email);
  };

  const handleOtpSubmit = async (e) => {
    e.preventDefault();
    const success = await verifyOTP();
    if (success) {
      onLogin();
    }
  };

  return (
    <div className="auth-page">
      <div className="auth-container">
        <h1>Welcome to Thera</h1>

        {error && <div className="error-message">{error}</div>}

        {step === "email" ? (
          <form onSubmit={handleEmailSubmit}>
            <div className="form-group">
              <label htmlFor="email">Email address</label>
              <input
                id="email"
                type="email"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                placeholder="your@getthera.com"
                required
              />
            </div>
            <button type="submit" disabled={loading}>
              {loading ? "Sending..." : "Send OTP"}
            </button>
          </form>
        ) : (
          <form onSubmit={handleOtpSubmit}>
            <div className="form-group">
              <label htmlFor="otp">Enter verification code</label>
              <input
                id="otp"
                type="text"
                value={otp}
                onChange={(e) => setOtp(e.target.value)}
                placeholder="Enter the code sent to your email"
                required
              />
            </div>
            <button type="submit" disabled={loading}>
              {loading ? "Verifying..." : "Verify Code"}
            </button>
          </form>
        )}
      </div>
    </div>
  );
}

export default AuthPage;
