import streamlit as st
from datetime import date, timedelta
st.set_page_config(page_title="Shopify Connector - Date Filter", layout="wide")

# --------- Helper Functions ---------
def get_date_range(option: str):
    today = date.today()
    if option == "Today":
        return today, today
    elif option == "Yesterday":
        yest = today - timedelta(days=1)
        return yest, yest
    elif option == "Last 7 days":
        return today - timedelta(days=6), today
    elif option == "Last 30 days":
        return today - timedelta(days=29), today
    elif option == "Last 60 days":
        return today - timedelta(days=59), today
    elif option == "Last 90 days":
        return today - timedelta(days=89), today
    elif option == "Last 365 days":
        return today - timedelta(days=364), today
    elif option == "Last week":
        start = today - timedelta(days=today.weekday() + 7)
        end = start + timedelta(days=6)
        return start, end
    elif option == "Last month":
        first = today.replace(day=1)
        last_month_end = first - timedelta(days=1)
        last_month_start = last_month_end.replace(day=1)
        return last_month_start, last_month_end
    elif option == "Last quarter":
        quarter = (today.month - 1) // 3 + 1
        start_month = 3 * (quarter - 2) + 1
        if start_month < 1: start_month += 12
        year = today.year if quarter > 1 else today.year - 1
        start = date(year, start_month, 1)
        end = (date(year, start_month + 3, 1) - timedelta(days=1)
               if start_month < 10 else date(year, 12, 31))
        return start, end
    elif option == "Week to date":
        start = today - timedelta(days=today.weekday())
        return start, today
    elif option == "Last year":
        start = date(today.year - 1, 1, 1)
        end = date(today.year - 1, 12, 31)
        return start, end
    elif option == "Month to date":
        start = today.replace(day=1)
        return start, today
    elif option == "Quarter to date":
        quarter = (today.month - 1) // 3 + 1
        start_month = 3 * (quarter - 1) + 1
        start = date(today.year, start_month, 1)
        return start, today
    elif option == "Year to date":
        start = date(today.year, 1, 1)
        return start, today
    else:  # Custom
        st.date_input(value=(start, end),key="custom_date")
        return start, end


# --------- Sidebar Filter with Quick Buttons ---------

        

        



