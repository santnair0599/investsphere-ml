"""Generate the documents/*.pdf source files (policy + research, as real PDFs).

These are the unstructured sources for the RAG knowledge base (Databricks AI Search).
Uses fpdf2:  pip install fpdf2

Run:  python data/generate_documents.py
"""
import os

from fpdf import FPDF

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "documents")

# doc file name -> (title, list of paragraphs)
DOCS = {
    "investment_policy_statement.pdf": (
        "Investment Policy Statement (IPS)",
        [
            "Section 1. Purpose. This Investment Policy Statement governs all portfolios, "
            "including the Gulf Equity Fund (GULF_EQ), the Global Balanced Fund (GLOBAL_BAL) "
            "and the Income Fixed Income portfolio (INCOME_FI).",
            "Section 4.2 Sector concentration. No single sector shall exceed 35% of total "
            "portfolio market value for any fund. In addition, for the Gulf Equity Fund "
            "(GULF_EQ), exposure to the Banking sector shall not exceed 20% of market value. "
            "A portfolio breaching this threshold must be rebalanced within five business days.",
            "Section 4.3 Issuer concentration. Exposure to any single issuer shall not exceed "
            "25% of portfolio market value across all funds, aggregated across equity and debt.",
            "Section 4.4 Counterparty limits. Volume routed through any single HIGH-risk "
            "counterparty shall not exceed 15% of executed volume.",
            "Section 4.5 Technology limit. For the Global Balanced Fund (GLOBAL_BAL), exposure "
            "to the Technology sector shall not exceed 40% of portfolio market value.",
            "Section 5. Breach handling. Breaches are recorded in the limit-breach register; "
            "Banking-sector breaches (Section 4.2) are treated as high priority.",
        ],
    ),
    "listed_equity_research_note.pdf": (
        "Listed Equity Research Note: Emirates NBD",
        [
            "Asset: Emirates NBD (ISIN AE_ENBD, issuer ISS_ENBD). Sector: Banking. Country: UAE.",
            "Emirates NBD remains one of the largest UAE banking groups with a strong retail "
            "franchise and improving net interest margins. Asset quality is constructive; "
            "concentration in UAE real-estate lending is a watch item.",
            "Risk note: as a Banking name, large Emirates NBD holdings contribute to both the "
            "Banking sector limit (IPS 4.2) and the single-issuer limit (IPS 4.3). Portfolios "
            "with heavy positions should be monitored for breaches of both.",
        ],
    ),
    "private_investment_committee_memo.pdf": (
        "Private Investment Committee Memo: Gulf Logistics Holding",
        [
            "Asset: Gulf Logistics Holding (id PE_GULFLOG, issuer ISS_GULFLOG). Asset class: "
            "Private Equity. Sector: Industrials. Currency: USD.",
            "The committee approved a follow-on commitment. The latest fair value is USD 5.2m "
            "(up from USD 5.0m), marked quarterly from the private valuation snapshot.",
            "Liquidity note: private assets are illiquid and valued periodically, not daily. "
            "They are excluded from listed-market exposure calculations and tracked separately "
            "in the valuation snapshots; see the portfolio risk guidelines for treatment.",
        ],
    ),
    "portfolio_risk_guidelines.pdf": (
        "Portfolio Risk Guidelines",
        [
            "Concentration risk. Sector, issuer and counterparty concentration limits are "
            "defined in the IPS and enforced by the limit-breach register.",
            "Liquidity risk. Private equity and fund holdings are illiquid; valuations are "
            "periodic. Maintain sufficient liquid listed assets to meet capital calls.",
            "Currency risk. Multi-currency portfolios convert to base currency using the "
            "currency_rates reference; monitor unhedged foreign-currency exposure.",
            "Data quality. Exposure and breach reporting depend on complete holdings and "
            "prices; missing prices or stale feeds must be flagged before sign-off.",
        ],
    ),
}


def build(file_name, title, paragraphs):
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 16)
    pdf.multi_cell(0, 10, title)
    pdf.ln(4)
    pdf.set_font("Helvetica", "", 12)
    for para in paragraphs:
        pdf.multi_cell(0, 7, para)
        pdf.ln(2)
    pdf.output(os.path.join(OUT, file_name))
    print(f"  documents/{file_name}")


def main():
    os.makedirs(OUT, exist_ok=True)
    for file_name, (title, paragraphs) in DOCS.items():
        build(file_name, title, paragraphs)
    print("Done. 4 PDFs written under data/documents/.")


if __name__ == "__main__":
    main()
