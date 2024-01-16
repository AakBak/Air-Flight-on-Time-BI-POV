import streamlit as st
from st_pages import Page, show_pages

st.set_page_config(
    page_title="Business Intelligence",
    page_icon="🧠",
    layout="wide",
    initial_sidebar_state="auto",
    menu_items=None
)

show_pages([
    Page("pages/problem-statement.py", "On-Time Flight Study", "🔍"),
    Page("app.py", "Data Pre-Processing", "⌛"),
    Page("pages/feature_engineering.py", "Feature Engineering", "🛠️"),  
    Page("pages/model.py", "Modelling", "💡")
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

# Problem Statement and Specific Objectives
st.title(':red[Airline On-Time Performance]')

## Problem Statement
st.header('🛫 Problem Statement:')
st.write('The aviation industry aims to enhance on-time performance, operational efficiency, and passenger satisfaction. '
         'This app conducts a comparative analysis of airline on-time data for 1991 and 2001, identifying key '
         'characteristics of on-time flights and addressing evolving dynamics over the years. Ethical concerns related '
         'to data collection initiatives at airports are also discussed based on Solove\'s (2006) framework.')

## Specific Objectives
st.header('🎯 Specific Objectives:')
st.write('1. Compare and contrast the characteristics of on-time flights in the airline datasets for 1991 and 2001.'
         '\n2. Identify and analyze additional analytics, including predictive and prescriptive insights, '
         'to contribute to actionable strategies for improving on-time performance.'
         '\n3. Assess potential ethical dilemmas and challenges associated with proposed data collection initiatives '
         'at airports, considering Solove\'s (2006) framework for ethical data practices.')

# Additional Analytics
st.header('📊 Additional Analytics:')
## Predictive Question
st.subheader('🔮 Predictive Question:')
st.write('Can historical data accurately predict flight delays, and if so, how effective are machine learning models in '
         'achieving this?')

### Predictive Answer
st.write('Yes, we can predict flight delays with a high degree of accuracy using machine learning models based on input parameters, '
         'particularly XGBoost. The predictive models, when applied to the datasets of 1991 and 2001, demonstrated promising accuracies. '
         'For the 1991 dataset, the XGBoost model achieved an accuracy of 86.74%, and for the 2001 dataset, an accuracy of 89.18%. '
         'These results suggest that historical data, including features such as departure delay, actual elapsed time, and distance, '
         'can be leveraged effectively to predict flight delays.')

## Prescriptive Question
st.subheader('🚀 Prescriptive Question:')
st.write('What actionable insights can be derived from the analysis to minimize flight delays and enhance overall '
         'on-time performance in the aviation industry?')

### Prescriptive Answer
st.write('Based on the feature importance analysis, several actionable insights can be derived to minimize flight delays:'
         '\n- **Mitigate Departure Delays:** Given its significant impact, efforts should be directed towards minimizing departure delays. '
         'Implementing operational strategies to ensure timely departures, such as efficient boarding processes and gate management, '
         'can contribute to on-time arrivals.'
         '\n- **Optimize Actual Elapsed Time:** Strategies to optimize actual elapsed time, especially for longer flights, can positively '
         'influence on-time performance. This may involve streamlining in-flight processes or adjusting schedules to better align with optimal travel durations.'
         '\n- **Consider Flight Distance:** Shorter flight distances are associated with higher chances of on-time arrivals. Airlines could '
         'explore route optimizations or adjust schedules to prioritize shorter distances where feasible.'
         '\n- **Operational Focus on TaxiIn and TaxiOut:** In the 2001 dataset, the inclusion of TaxiIn and TaxiOut as crucial features indicates '
         'the importance of ground operations. Airlines and airports can focus on improving efficiency in taxiing processes, potentially through better '
         'runway utilization or optimizing ground handling procedures.')

# Ethical Considerations
st.header('🤔 Ethical Considerations:')
st.write('The integration of advanced technologies at an airport, involving the collection and categorization of traveler data, as well as the '
         'implementation of features such as reminders and facial recognition systems, raises ethical considerations based on Solove (2006). Here is '
         'a discussion of potential ethical dilemmas and issues:')

## Information Collection
st.subheader('**Information Collection:**')
st.write('- The collection of data from travelers\' smartphones, flight plans, passport control, and security control necessitates transparency '
         'and informed consent. It is crucial to clearly communicate the types of data being collected and how it will be utilized.')

## Data Categorization
st.subheader('**Data Categorization:**')
st.write('- Categorizing travelers based on the likelihood of causing flight delays introduces concerns about profiling. Ensuring fairness and '
         'accuracy in the categorization process is essential to prevent biases or discrimination. Transparency and accountability in the '
         'categorization algorithm are crucial.')

## Communication and Reminders
st.subheader('**Communication and Reminders:**')
st.write('- Sending reminders to travelers categorized as likely to cause late departures raises privacy and consent issues. Ensuring passengers are '
         'well-informed about categorization criteria and providing opt-out options is crucial to respect individual autonomy.')

## Staff Access to Dashboard
st.subheader('**Staff Access to Dashboard:**')
st.write('- Providing airport staff with access to a dashboard indicating potential traveler locations raises surveillance concerns. Clear guidelines '
         'on access, usage, and preventing misuse are necessary to uphold privacy standards.')

## Facial Recognition in Public Spaces
st.subheader('**Facial Recognition in Public Spaces:**')
st.write('- Implementing facial recognition in public spaces raises privacy concerns. Safeguarding biometric data, obtaining consent, and adhering to '
         'legal and ethical standards are crucial to prevent unauthorized surveillance.')

st.subheader('📚 Overall Ethical Reflection:')
st.write('1. **Privacy and Consent:** Transparency and informed consent are critical for respecting travelers\' privacy.'
         '\n2. **Fairness and Non-Discrimination:** Categorization should avoid biases to ensure fair treatment of all travelers.'
         '\n3. **Data Security and Access Control:** Robust security measures and access controls are necessary to prevent data misuse.'
         '\n4. **Legal Compliance:** Adherence to privacy laws is essential to protect individuals\' rights.'
         '\n5. **Public Awareness and Communication:** Open communication about data practices builds trust and enables informed decision-making.')
