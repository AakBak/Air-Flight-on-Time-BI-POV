import streamlit as st
from st_pages import Page, show_pages, Section

st.set_page_config(
    page_title="Business Intelligence",
    page_icon="🧠",
    layout="wide",
    initial_sidebar_state="auto",
    menu_items=None
)

show_pages([
    Page("pages/problem-statement.py", "Problem Statement", "🔍"),
    Page("app.py", "Data Pre-Processing", "⌛"),  
    Page("pages/model.py", "Modelling", "💡"),
    ])

video_source = "https://assets.mixkit.co/videos/preview/mixkit-airplane-flying-in-a-red-cloudy-sky-1146-large.mp4"

# Use the HTML video tag inside a st.markdown element
st.markdown(f"""
<div style="position: relative;">
<video autoplay loop muted style="width: 100%; height: 150px; object-fit: cover;">
<source src="{video_source}" type="video/mp4">
</video>
<div style="position: absolute; top: 0; left: 0; width: 100%; height: 100%; display: flex; align-items: center; justify-content: center; font-size: 48px; font-family: Arial; text-shadow: 2px 2px 4px black; background: linear-gradient(to right, white, #f2f2f2, #e6e6e6, #d9d9d9, #cccccc, #bfbfbf, #b3b3b3); -webkit-background-clip: text; color: transparent;">
    1991 | 2001 Flights ✈️
</div>
<div style="position: absolute; bottom: 0; right: 0; margin: 10px; font-size: 14px; font-family: Arial; color: white;">
    <i>by</i> <b>Abed Bakkour</b>
</div>
</div>
""", unsafe_allow_html=True)

st.divider()

## Problem Statement
st.header("🔍 Problem Statement:")

st.markdown("""
The airline industry constantly strives to enhance on-time performance, considering it a critical factor in customer satisfaction and operational efficiency. In the context of the provided data sets for the years 1991 and 2001, there is a need to analyze and compare the characteristics of flights that were on time in these two distinct periods. Additionally, the introduction of a comprehensive data collection system at a larger airport raises ethical concerns, particularly in the context of privacy, as outlined by Solove (2006).
""")

## Specific Objectives
st.header("🎯 Specific Objectives:")

st.markdown("""
1. **Comparison of On-Time Performance (1991 vs. 2001):**
   - Identify key factors characterizing flights that were on time in 1991 and 2001.
   - Analyze trends and patterns in on-time performance across the two years.
   - Assess any notable changes or similarities between the two time periods.

2. **Additional Analytics Question:**
   - Pose and answer a relevant analytics question, either predictive or prescriptive, to gain deeper insights into on-time performance.

3. **Ethical Considerations in Data Collection at Airports:**
   - Evaluate potential ethical dilemmas and problems that may arise from the planned data collection initiative at the larger airport.
   - Apply Solove's taxonomy of privacy to assess the implications of collecting data from travelers' smartphones, flight plans, passport control, security control, and their interactions with shops and restaurants.
   - Consider the ethical implications of implementing facial recognition systems in commercial spaces within the airport.
""")

## Significance of the Study
st.header("🌐 Significance of the Study:")

st.markdown("""
Understanding the characteristics of on-time flights in different years can provide valuable insights for airlines in optimizing their operations and improving punctuality. Additionally, the ethical evaluation of data collection practices is crucial to ensuring the protection of passengers' privacy rights and avoiding potential legal and reputational issues for the airport.
""")

## Expected Outcomes
st.header("📈 Expected Outcomes:")

st.markdown("""
1. A comprehensive analysis of on-time performance characteristics in 1991 and 2001.
2. Insights from an additional analytics question, contributing to a better understanding of factors influencing on-time performance.
3. Identification of potential ethical dilemmas and privacy concerns associated with the proposed data collection initiatives at the larger airport.
""")

## Research Approach
st.header("🔍 Research Approach:")

st.markdown("""
1. **Data Analysis:** Utilize statistical and exploratory data analysis techniques to compare on-time performance in 1991 and 2001.
2. **Analytics Modeling:** Employ predictive or prescriptive analytics techniques to answer the additional analytics question.
3. **Ethical Evaluation:** Apply Solove's taxonomy of privacy to systematically assess the ethical implications of the proposed data collection initiatives at the airport.
""")

## Scope and Limitations
st.header("🔒 Scope and Limitations:")

st.markdown("""
This study is limited to the analysis of on-time performance in the specified years and the ethical considerations of data collection at airports. The findings may be influenced by the availability and quality of the data sets and the specific context of the airport in question.
""")

## Research Timeline
st.header("🗓️ Research Timeline:")

st.markdown("""
The research will be conducted over a specified time frame, encompassing data collection, analysis, and ethical evaluation. Regular progress assessments will be conducted to ensure timely completion of the study.
""")

## Deliverables
st.header("📦 Deliverables:")

st.markdown("""
1. Comparative analysis of on-time performance.
2. Findings from the additional analytics question.
3. Ethical assessment report based on Solove's taxonomy.
""")
