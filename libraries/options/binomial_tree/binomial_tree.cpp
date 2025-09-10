#include<iostream>
#include<cmath>
#include<stdexcept>
#include<pybind11/pybind11.h>

namespace py = pybind11;

/**
 * Binomial Option Pricing (Cox–Ross–Rubinstein model)
 *
 * Formulas:
 *   u = exp(σ * sqrt(Δt))         (up factor)
 *   d = 1/u                       (down factor)
 *   p = (exp((r - q) * Δt) - d) / (u - d)   (risk-neutral probability)
 *
 * Backward induction:
 *   V_t = max( intrinsic(S_t), discounted_expectation )
 *   where discounted_expectation = e^(-r Δt) * [ p*V_up + (1-p)*V_down ]
 *
 * Parameters:
 *   S     - spot price of underlying asset
 *   K     - strike price of option
 *   T     - time to maturity (in years)
 *   r     - risk-free interest rate (continuously compounded)
 *   q     - dividend yield (continuously compounded)
 *   sigma - volatility of underlying asset
 *   steps - number of binomial partitions (N)
 */


/**
 * Binomial American Call Option Price (CRR model)
 */
double call_price(double S, double K, double T,
                     double r, double q, double sigma, int steps) {
    double dt = T / steps;
    double u = exp(sigma * sqrt(dt));
    double d = 1.0 / u;
    double disc = exp(-r * dt);
    double p = (exp((r - q) * dt) - d) / (u - d);

    if (p < 0.0 || p > 1.0) throw std::runtime_error("Arbitrage violation: check parameters.");

    // Terminal payoffs at maturity
    std::vector<double> values(steps + 1);
    for (int j = 0; j <= steps; ++j) {
        double ST = S * pow(u, steps - j) * pow(d, j);
        values[j] = std::max(ST - K, 0.0);
    }

    // Backward induction with early exercise check
    for (int i = steps - 1; i >= 0; --i) {
        for (int j = 0; j <= i; ++j) {
            double continuation = disc * (p * values[j] + (1.0 - p) * values[j + 1]);
            double ST = S * pow(u, i - j) * pow(d, j);
            double exercise = std::max(ST - K, 0.0);
            values[j] = std::max(continuation, exercise);  // American feature
        }
    }
    return values[0];
}


/**
 * Binomial American Put Option Price (CRR model)
 */
double put_price(double S, double K, double T,
                    double r, double q, double sigma, int steps) {
    double dt = T / steps;
    double u = exp(sigma * sqrt(dt));
    double d = 1.0 / u;
    double disc = exp(-r * dt);
    double p = (exp((r - q) * dt) - d) / (u - d);

    if (p < 0.0 || p > 1.0) throw std::runtime_error("Arbitrage violation: check parameters.");

    // Terminal payoffs at maturity
    std::vector<double> values(steps + 1);
    for (int j = 0; j <= steps; ++j) {
        double ST = S * pow(u, steps - j) * pow(d, j);
        values[j] = std::max(K - ST, 0.0);
    }

    // Backward induction with early exercise check
    for (int i = steps - 1; i >= 0; --i) {
        for (int j = 0; j <= i; ++j) {
            double continuation = disc * (p * values[j] + (1.0 - p) * values[j + 1]);
            double ST = S * pow(u, i - j) * pow(d, j);
            double exercise = std::max(K - ST, 0.0);
            values[j] = std::max(continuation, exercise);  // American feature
        }
    }
    return values[0];
}


/**
 * Implied Volatility Solver for Call (Binomial CRR)
 *
 * Uses bisection method to solve:
 *   BinomialCall(S, K, T, r, q, σ, steps) = MarketPrice
 *
 * Parameters:
 *   market_price - observed option price in market
 *   lo, hi       - search bounds for volatility
 *   tol          - tolerance for convergence
 *   maxit        - maximum iterations
 */
double implied_vol_call(double market_price, double S, double K, double T,
                        double r, double q, int steps,
                        double lo=1e-8, double hi=5.0,
                        double tol=1e-8, int maxit=100) {
    double mid = 0.5 * (lo + hi);
    double price  = 0.0;
    for (int i = 0; i < maxit; ++i) {
        mid = 0.5 * (lo + hi);
        price = call_price(S, K, T, r, q, mid, steps);
        if (fabs(price - market_price) < tol) return mid;
        if (price > market_price) hi = mid;
        else lo = mid;
    }
    return mid;
}


/**
 * Implied Volatility Solver for Put (Binomial CRR)
 *
 * Same method as call, applied to BinomialPut.
 */
double implied_vol_put(double market_price, double S, double K, double T,
                       double r, double q, int steps,
                       double lo=1e-8, double hi=5.0,
                       double tol=1e-8, int maxit=100) {
    double mid, price;
    for (int i = 0; i < maxit; ++i) {
        mid = 0.5 * (lo + hi);
        price = put_price(S, K, T, r, q, mid, steps);
        if (fabs(price - market_price) < tol) return mid;
        if (price > market_price) hi = mid;
        else lo = mid;
    }
    return mid;
}

PYBIND11_MODULE(binomial_tree, m) {
    m.doc() = "Binomial Tree Option Pricing (Cox-Ross-Rubinstein model)";

    m.def("call_price", &call_price, "Binomial American Call Option Price",
          py::arg("S"), py::arg("K"), py::arg("T"), py::arg("r"),
          py::arg("q"), py::arg("sigma"), py::arg("steps"));
    m.def("put_price", &put_price, "Binomial American Put Option Price",
          py::arg("S"), py::arg("K"), py::arg("T"), py::arg("r"),
          py::arg("q"), py::arg("sigma"), py::arg("steps"));
    m.def("implied_vol_call", &implied_vol_call, "Implied Volatility for Call (Binomial)",
          py::arg("market_price"), py::arg("S"), py::arg("K"), py::arg("T"),
          py::arg("r"), py::arg("q"), py::arg("steps"),
          py::arg("lo")=1e-8, py::arg("hi")=5.0,
          py::arg("tol")=1e-8, py::arg("maxit")=100);
    m.def("implied_vol_put", &implied_vol_put, "Implied Volatility for Put (Binomial)",
          py::arg("market_price"), py::arg("S"), py::arg("K"), py::arg("T"),
          py::arg("r"), py::arg("q"), py::arg("steps"),
          py::arg("lo")=1e-8, py::arg("hi")=5.0,
          py::arg("tol")=1e-8, py::arg("maxit")=100);
}