"""
Streamlit app to visualize onboarding funnel.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from data_loader import load_data_from_bigquery


def get_onboarding_funnel_data():
    """
    Get onboarding funnel data from BigQuery.
    """
    df = load_data_from_bigquery(use_view_query=True)

    # Filter for onboarding events only
    # onboarding = df[df['event_category_ordered_numbered'] == 1].copy()

    # Count unique users per onboarding step
    funnel_data = (
        df.groupby("event_category_ordered")
        .agg(users=("user_id", "nunique"))
        .reset_index()
        .sort_values("users", ascending=False)
    )

    return funnel_data


def create_funnel_chart(funnel_data):
    """
    Create a plotly funnel chart for onboarding.
    """
    # Define the expected funnel order
    funnel_order = [
        "1.onboarding_main",
        "1.onboarding_loading",
        "1.onboarding_pick_genres",
        "1.onboarding_link_streaming",
        "1.onboarding_callback",
        "1.onboarding_intro",
        "1.onboarding_pick_artists",
        "1.onboarding_close",
    ]

    # Filter and order the data
    funnel_steps = []
    for step in funnel_order:
        row = funnel_data[funnel_data["event_category_ordered"] == step]
        if not row.empty:
            funnel_steps.append(
                {
                    "step": step.replace("1.onboarding_", "").replace("_", " ").title(),
                    "users": row["users"].iloc[0],
                }
            )

    funnel_df = pd.DataFrame(funnel_steps)

    if not funnel_df.empty:
        # Calculate conversion rates
        baseline = funnel_df["users"].iloc[0]
        funnel_df["conversion_rate"] = (funnel_df["users"] / baseline * 100).round(1)

        # Create funnel chart
        fig = go.Figure(
            go.Funnel(
                y=funnel_df["step"],
                x=funnel_df["users"],
                textposition="inside",
                textinfo="value+percent initial",
                opacity=0.65,
                marker={
                    "color": [
                        "#1f77b4",
                        "#ff7f0e",
                        "#2ca02c",
                        "#d62728",
                        "#9467bd",
                        "#8c564b",
                    ],
                    "line": {"width": 2, "color": "white"},
                },
                connector={"line": {"color": "royalblue", "dash": "dot", "width": 3}},
            )
        )

        fig.update_layout(
            title={
                "text": "Onboarding Funnel Analysis",
                "x": 0.5,
                "xanchor": "center",
                "font": {"size": 24, "color": "#1f77b4"},
            },
            height=600,
            showlegend=False,
            font=dict(size=14),
        )

        return fig, funnel_df

    return None, None


def main():
    st.set_page_config(page_title="Lissen - Onboarding", page_icon="📊", layout="wide")

    st.title("Lissen - Onboarding")
    st.markdown("---")

    # Load data
    with st.spinner("Loading data from BigQuery..."):
        funnel_data = get_onboarding_funnel_data()

    fig, funnel_df = create_funnel_chart(funnel_data)

    if fig:
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("Step-by-Step Breakdown")
        # Calculate drop-off
        funnel_df["drop_off"] = -funnel_df["users"].diff().fillna(0).astype(int)
        funnel_df["drop_off_pct"] = (
            funnel_df["drop_off"] / funnel_df["users"].shift(1) * 100
        ).fillna(0)

        st.dataframe(
            funnel_df[
                ["step", "users", "conversion_rate", "drop_off", "drop_off_pct"]
            ].style.format(
                {
                    "conversion_rate": "{:.1f}%",
                    "drop_off_pct": "{:.1f}%",
                    "drop_off": "{:d}",
                }
            ),
            use_container_width=True,
        )

    else:
        st.warning("No funnel data available")


if __name__ == "__main__":
    main()
