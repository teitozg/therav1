import { useState } from "react";
import { createClient } from "@supabase/supabase-js";

const supabaseUrl = process.env.REACT_APP_SUPABASE_URL;
const supabaseAnonKey = process.env.REACT_APP_SUPABASE_ANON_KEY;

const supabase = createClient(supabaseUrl, supabaseAnonKey);

export function useAuth() {
  const [email, setEmail] = useState("");
  const [otp, setOtp] = useState("");
  const [step, setStep] = useState("email");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const isAllowedEmail = (email) => {
    const allowedEmails = ["teozavalia@gmail.com", "nachoberardi@gmail.com"];
    return email.endsWith("@getthera.com") || allowedEmails.includes(email);
  };

  const sendOTP = async (email) => {
    if (!supabaseUrl || !supabaseAnonKey) {
      setError(
        "Authentication service not configured. Please contact support."
      );
      return;
    }

    setLoading(true);
    setError(null);

    try {
      if (!isAllowedEmail(email)) {
        throw new Error("Only getthera.com accounts are allowed.");
      }

      const { error } = await supabase.auth.signInWithOtp({
        email,
        options: {
          shouldCreateUser: true,
          emailRedirectTo: window.location.origin,
          type: "otp",
        },
      });

      if (error) throw error;
      setStep("otp");
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const verifyOTP = async () => {
    if (!supabaseUrl || !supabaseAnonKey) {
      setError(
        "Authentication service not configured. Please contact support."
      );
      return false;
    }

    setLoading(true);
    setError(null);

    try {
      const { error, data } = await supabase.auth.verifyOtp({
        email,
        token: otp,
        type: "email",
      });

      if (error) throw error;

      // Set session expiry to 24 hours
      const expiresAt = new Date();
      expiresAt.setHours(expiresAt.getHours() + 24);

      localStorage.setItem(
        "session",
        JSON.stringify({
          email,
          expiresAt: expiresAt.toISOString(),
          accessToken: data.session.access_token,
        })
      );

      return true;
    } catch (err) {
      setError(err.message);
      return false;
    } finally {
      setLoading(false);
    }
  };

  return {
    email,
    setEmail,
    otp,
    setOtp,
    step,
    loading,
    error,
    sendOTP,
    verifyOTP,
  };
}
