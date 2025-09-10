#include <pybind11/pybind11.h>
#include <iostream>
#include <cmath> 
#include <algorithm>
#include <limits>

namespace py = pybind11;

// --- Normal PDF/CDF (fixed CDF) ---
inline double norm_cdf(double x) {
    // CDF = 0.5 * (1 + erf(x / sqrt(2)))
    return 0.5 * (1.0 + std::erf(x * M_SQRT1_2));
}
inline double norm_pdf(double x) {
    static const double INV_SQRT_2PI = 0.3989422804014337;
    return INV_SQRT_2PI * std::exp(-0.5 * x * x);
}

// --- Black–Scholes prices with dividend yield q (your variables) ---
inline double call_price(double S, double K, double T, double r, double sigma, double q = 0.0) {
    // At expiry, intrinsic
    if (T <= 0.0) return std::max(S - K, 0.0);

    const double disc_r = std::exp(-r * T);
    const double disc_q = std::exp(-q * T);

    // If sigma <= 0, price collapses to discounted forward intrinsic
    if (sigma <= 0.0) return std::max(S * disc_q - K * disc_r, 0.0);

    const double volT = sigma * std::sqrt(T);
    const double d1   = (std::log(S / K) + (r - q + 0.5 * sigma * sigma) * T) / volT;
    const double d2   = d1 - volT;

    return S * disc_q * norm_cdf(d1) - K * disc_r * norm_cdf(d2);
}

inline double put_price(double S, double K, double T, double r, double sigma, double q = 0.0) {
    if (T <= 0.0) return std::max(K - S, 0.0);

    const double disc_r = std::exp(-r * T);
    const double disc_q = std::exp(-q * T);

    if (sigma <= 0.0) return std::max(K * disc_r - S * disc_q, 0.0);

    const double volT = sigma * std::sqrt(T);
    const double d1   = (std::log(S / K) + (r - q + 0.5 * sigma * sigma) * T) / volT;
    const double d2   = d1 - volT;

    return K * disc_r * norm_cdf(-d2) - S * disc_q * norm_cdf(-d1);
}

// --- (Optional) vega with your variables (useful for Newton polish) ---
inline double vega_call(double S, double K, double T, double r, double sigma, double q = 0.0) {
    if (T <= 0.0 || sigma <= 0.0) return 0.0;
    const double disc_q = std::exp(-q * T);
    const double volT   = sigma * std::sqrt(T);
    const double d1     = (std::log(S / K) + (r - q + 0.5 * sigma * sigma) * T) / volT;
    return S * disc_q * norm_pdf(d1) * std::sqrt(T);
}

// --- Implied volatility (CALL) using bisection + 1 Newton polish ---
inline double implied_vol_call(double market_price, double S, double K, double T, double r,
                               double q = 0.0, double lo = 1e-8, double hi = 5.0,
                               double tol = 1e-8, int maxit = 80)
{
    // Sanity
    if (!(std::isfinite(market_price) && std::isfinite(S) && std::isfinite(K) &&
          std::isfinite(T) && std::isfinite(r) && std::isfinite(q)) ||
        S <= 0.0 || K <= 0.0 || T <= 0.0)
        return std::numeric_limits<double>::quiet_NaN();

    const double disc_r = std::exp(-r * T);
    const double disc_q = std::exp(-q * T);
    const double LB = std::max(S * disc_q - K * disc_r, 0.0);
    const double UB = S * disc_q;  // sigma -> inf

    if (market_price < LB - 1e-12 || market_price > UB + 1e-12)
        return std::numeric_limits<double>::quiet_NaN();
    if (std::fabs(market_price - LB) < 1e-12) return lo;

    auto f = [&](double s) { return call_price(S, K, T, r, s, q) - market_price; };

    double a = lo, b = hi, fa = f(a), fb = f(b);
    int expand = 0;
    while (fa * fb > 0.0 && expand < 20) { b *= 1.5; fb = f(b); ++expand; }
    if (fa * fb > 0.0) return std::numeric_limits<double>::quiet_NaN();

    for (int it = 0; it < maxit; ++it) {
        double m  = 0.5 * (a + b);
        double fm = f(m);
        if (std::fabs(fm) < tol || (b - a) < tol) {
            // Newton polish using call vega (skip if tiny)
            double v = vega_call(S, K, T, r, m, q);
            if (v > 1e-12) {
                double newton = m - fm / v;
                if (newton > 0.0 && newton < 10.0) m = newton;
            }
            return m;
        }
        if (fa * fm <= 0.0) { b = m; fb = fm; } else { a = m; fa = fm; }
    }
    return 0.5 * (a + b);
}

// --- Implied volatility (PUT) mirrors CALL version, with put bounds/price ---
inline double implied_vol_put(double market_price, double S, double K, double T, double r,
                              double q = 0.0, double lo = 1e-8, double hi = 5.0,
                              double tol = 1e-8, int maxit = 80)
{
    if (!(std::isfinite(market_price) && std::isfinite(S) && std::isfinite(K) &&
          std::isfinite(T) && std::isfinite(r) && std::isfinite(q)) ||
        S <= 0.0 || K <= 0.0 || T <= 0.0)
        return std::numeric_limits<double>::quiet_NaN();

    const double disc_r = std::exp(-r * T);
    const double disc_q = std::exp(-q * T);
    const double LB = std::max(K * disc_r - S * disc_q, 0.0);
    const double UB = K * disc_r;  // sigma -> inf

    if (market_price < LB - 1e-12 || market_price > UB + 1e-12)
        return std::numeric_limits<double>::quiet_NaN();
    if (std::fabs(market_price - LB) < 1e-12) return lo;

    auto f = [&](double s) { return put_price(S, K, T, r, s, q) - market_price; };

    double a = lo, b = hi, fa = f(a), fb = f(b);
    int expand = 0;
    while (fa * fb > 0.0 && expand < 20) { b *= 1.5; fb = f(b); ++expand; }
    if (fa * fb > 0.0) return std::numeric_limits<double>::quiet_NaN();

    for (int it = 0; it < maxit; ++it) {
        double m  = 0.5 * (a + b);
        double fm = f(m);
        if (std::fabs(fm) < tol || (b - a) < tol) {
            // Reuse call vega (same formula for vega)
            double v = vega_call(S, K, T, r, m, q);
            if (v > 1e-12) {
                double newton = m - fm / v;
                if (newton > 0.0 && newton < 10.0) m = newton;
            }
            return m;
        }
        if (fa * fm <= 0.0) { b = m; fb = fm; } else { a = m; fa = fm; }
    }
    return 0.5 * (a + b);
}

PYBIND11_MODULE(blackscholes, m) {
    m.def("call_price", &call_price, "Call price", py::arg("S"), py::arg("K"), py::arg("T"),
          py::arg("r"), py::arg("sigma"), py::arg("q") = 0.0);
    m.def("put_price", &put_price, "Put price", py::arg("S"), py::arg("K"), py::arg("T"),
          py::arg("r"), py::arg("sigma"), py::arg("q") = 0.0);
    m.def("implied_vol_call", &implied_vol_call, "Call implied vol",
          py::arg("market_price"), py::arg("S"), py::arg("K"), py::arg("T"),
          py::arg("r"), py::arg("q") = 0.0,
          py::arg("lo")=1e-8, py::arg("hi")=5.0, py::arg("tol")=1e-8, py::arg("maxit")=80);
    m.def("implied_vol_put", &implied_vol_put, "Put implied vol",
          py::arg("market_price"), py::arg("S"), py::arg("K"), py::arg("T"),
          py::arg("r"), py::arg("q") = 0.0,
          py::arg("lo")=1e-8, py::arg("hi")=5.0, py::arg("tol")=1e-8, py::arg("maxit")=80);
}
