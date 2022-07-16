import pandas as pd

"""## Create DataFrame"""
df = pd.read_csv("data.csv")
df.drop(['Account Code', 'Ring Group'], axis=1, inplace=True)
df.rename(columns={'source chanel': 'src-chan', 'dst chanel': 'dst-chan'}, inplace=True)

"""reformat duration of calls"""

df['time'] = df['time'].astype(str).str.replace(r' \(.*', '', regex=True)
df['time'] = df.time.astype(str).str.replace('s', '')
df['time'] = pd.to_numeric(df["time"])

"""for loop extensions"""

extensions = pd.read_csv("extensions.csv")
export_dict = {'کاربران': [], 'داخلی': [], 'تعداد ورودی': [], 'تعداد ورودی پاسخ داده شده': [], 'درصد پاسخدهی': [],
               'تعداد ورودی بی پاسخ': [], 'زمان ورودی داخلی': [], 'متوسط مدت زمان مکالمات ورودی (دقیقه)': [],
               'تعداد خروجی': [],
               'زمان خروجی (دقیقه)': [], 'متوسط مدت زمان تماس های خروجی (دقیقه)': [],
               'مجموع مدت زمان تماس های ورودی و خروجی (دقیقه)': [], 'متوسط مدت زمان کل (دقیقه)': [],
               'مجموع مدت زمان ورودی و '
               'خروجی (ساعت)': []}

for index, extension in extensions.iterrows():
    extension_number = extension['ext']
    extension_name = extension['name']

    df_extension = df.loc[(df['source'] == extension_number) | (df['destination'] == extension_number)]

    """incoming"""

    incoming = df_extension.loc[(df['destination'] == extension_number) & (
            (df_extension['src-chan'].str.startswith('SIP/sip')) | (
                df_extension['src-chan'].str.contains('from-queue')))]
    incoming_calls = len(incoming)
    answered_calls = len(incoming.loc[incoming['condition'] == 'ANSWERED'])
    answered_percent = answered_calls * 100 / incoming_calls if incoming_calls > 0 else 0
    missed_calls = incoming_calls - answered_calls
    incoming_calls_time = sum(incoming.loc[(incoming['destination'] == extension_number) & (
            (incoming['src-chan'].str.startswith('SIP/sip')) | (incoming['src-chan'].str.contains('from-queue'))) & (
                                                   incoming['condition'] == 'ANSWERED')]['time']) // 60
    ave_incoming_calls_time = incoming_calls_time / answered_calls if answered_calls > 0 else 0

    """outgoing"""

    outgoing = df_extension.loc[
        (df_extension['source'] == extension_number) & (df_extension['dst-chan'].str.startswith('SIP/sip'))]
    outgoing_calls = len(outgoing)
    outgoing_calls_time = sum(outgoing['time']) // 60 if outgoing_calls > 0 else 0
    ave_outgoing_calls_time = incoming_calls_time / answered_calls if answered_calls > 0 else 0

    """overall"""

    total_inc_and_out_calls_time_minute = incoming_calls_time + outgoing_calls_time
    total_ave_time = (incoming_calls + outgoing_calls) / total_inc_and_out_calls_time_minute \
        if total_inc_and_out_calls_time_minute > 0 else 0
    total_inc_and_out_calls_time_hour = total_inc_and_out_calls_time_minute / 60\
        if total_inc_and_out_calls_time_minute > 0 else 0

    """export data"""
    export_dict['کاربران'].append(extension_name)
    export_dict['داخلی'].append(extension_number)
    export_dict['تعداد ورودی'].append(incoming_calls)
    export_dict['تعداد ورودی پاسخ داده شده'].append(answered_calls)
    export_dict['درصد پاسخدهی'].append(answered_percent)
    export_dict['تعداد ورودی بی پاسخ'].append(missed_calls)
    export_dict['زمان ورودی داخلی'].append(incoming_calls_time)
    export_dict['متوسط مدت زمان مکالمات ورودی (دقیقه)'].append(ave_incoming_calls_time)
    export_dict['تعداد خروجی'].append(outgoing_calls)
    export_dict['زمان خروجی (دقیقه)'].append(outgoing_calls_time)
    export_dict['متوسط مدت زمان تماس های خروجی (دقیقه)'].append(ave_outgoing_calls_time)
    export_dict['مجموع مدت زمان تماس های ورودی و خروجی (دقیقه)'].append(total_inc_and_out_calls_time_minute)
    export_dict['متوسط مدت زمان کل (دقیقه)'].append(total_ave_time)
    export_dict['مجموع مدت زمان ورودی و خروجی (ساعت)'].append(total_inc_and_out_calls_time_hour)
    print(f"extension {extension_number} created")

    """print data"""
df2 = pd.DataFrame.from_dict(export_dict)
df2.to_excel('export.xlsx', index=False)

input("press enter to exit")
