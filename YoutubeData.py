# Python project Youtube data Harvesting
# Author:Vi.S.Senthilkumar

import pandas as pd
import numpy as np
from dateutil import parser
import isodate

# Data visualization libraries
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import seaborn as sns
sns.set(style="darkgrid", color_codes=True)
import pymongo
from pymongo import MongoClient

# Google API
from googleapiclient.discovery import build
import streamlit as st

import isodate

# Data visualization libraries
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import seaborn as sns
sns.set(style="darkgrid", color_codes=True)
import  googleapiclient.discovery


# Google API
#from googleapiclient.discovery import build
youtube = googleapiclient.discovery.build('youtube', 'v3', developerKey='AIzaSyCeZxYGgE7L6tl2J1v6lWbfS0BqUdWMACM') #'AIzaSyB-4NIQtecQPbRX7TWKphThkb9_Brh2wL4')

import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
nltk.download('stopwords')
nltk.download('punkt')
from wordcloud import WordCloud

from pytube import YouTube


api_key ='AIzaSyDhs3UsgrIGLjcYImNB6UjJDgYOp3WH3c8' #'AIzaSyB-4NIQtecQPbRX7TWKphThkb9_Brh2wL4'


def get_channel_stats(youtube, channel_ids):
    """
    Get channel statistics: title, subscriber count, view count, video count, upload playlist
    Params:

    youtube: the build object from googleapiclient.discovery
    channels_ids: list of channel IDs

    Returns:
    Dataframe containing the channel statistics for all channels in the provided list: title, subscriber count, view count, video count, upload playlist

    """
    all_data = []
    request = youtube.channels().list(
        part='snippet,contentDetails,statistics,status',
        id=','.join(channel_ids))
    response = request.execute()

    for i in range(len(response['items'])):
        data = dict(channelName=response['items'][i]['snippet']['title'],
                    chennai_Id =response['items'][i]['id'],
                    subscribers=response['items'][i]['statistics']['subscriberCount'],
                    views=response['items'][i]['statistics']['viewCount'],
                    totalVideos=response['items'][i]['statistics']['videoCount'],
                    description=response['items'][i]['snippet']['description'],
                    chennal_status=response['items'][i]['status']['privacyStatus'],
                    playlistId=response['items'][i]['contentDetails']['relatedPlaylists']['uploads'])
        all_data.append(data)

    return pd.DataFrame(all_data),all_data



def get_video_ids(youtube, playlist_id):
    """
    Get list of video IDs of all videos in the given playlist
    Params:

    youtube: the build object from googleapiclient.discovery
    playlist_id: playlist ID of the channel

    Returns:
    List of video IDs of all videos in the playlist

    """

    request = youtube.playlistItems().list(
        part='snippet,contentDetails',
        playlistId=playlist_id,
        maxResults=50)
    response = request.execute()
    #print(response)
    video_ids = []
    playlist_details = []

    for i in range(len(response['items'])):
        video_ids.append(response['items'][i]['contentDetails']['videoId'])
        data = dict(video_id = response['items'][i]['contentDetails']['videoId'],
                    playlist_id=response['items'][i]['id'],
                    channel_ID=response['items'][i]['snippet']['channelId'],
                    Playlistname=response['items'][i]['snippet']['title'])
        playlist_details.append(data)
    next_page_token = response.get('nextPageToken')
    more_pages = True

    while more_pages:
        if next_page_token is None:
            more_pages = False
        else:
            request = youtube.playlistItems().list(
                part='snippet,contentDetails',
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token)
            response = request.execute()

            for i in range(len(response['items'])):
                video_ids.append(response['items'][i]['contentDetails']['videoId'])

                data = dict(video_id = response['items'][i]['contentDetails']['videoId'],
                             playlist_id=response['items'][i]['id'],
                             channel_ID=response['items'][i]['snippet']['channelId'],
                             Playlistname=response['items'][i]['snippet']['title'])
                playlist_details.append(data)
            next_page_token = response.get('nextPageToken')
    #print(playlist_details)
    return video_ids,playlist_details


def get_video_details(youtube, video_ids):
    """
    Get video statistics of all videos with given IDs
    Params:

    youtube: the build object from googleapiclient.discovery
    video_ids: list of video IDs

    Returns:
    Dataframe with statistics of videos, i.e.:
        'channelTitle', 'title', 'description', 'tags', 'publishedAt'
        'viewCount', 'likeCount', 'favoriteCount', 'commentCount'
        'duration', 'definition', 'caption'
    """

    all_video_info = []

    for i in range(0, len(video_ids), 50):
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=','.join(video_ids[i:i + 50])
        )
        response = request.execute()

        for video in response['items']:
            stats_to_keep = {'snippet': ['channelTitle', 'title', 'description', 'tags', 'publishedAt'],
                             'statistics': ['viewCount', 'likeCount', 'favouriteCount', 'commentCount'],
                             'contentDetails': ['duration', 'definition', 'caption']
                             }
            video_info = {}
            video_info['video_id'] = video['id']

            for k in stats_to_keep.keys():
                for v in stats_to_keep[k]:
                    try:
                        video_info[v] = video[k][v]
                    except:
                        video_info[v] = None

            all_video_info.append(video_info)

    return pd.DataFrame(all_video_info),all_video_info



def get_comments_in_videos(youtube, video_ids):
    """
    Get top level comments as text from all videos with given IDs (only the first 10 comments due to quote limit of Youtube API)
    Params:

    youtube: the build object from googleapiclient.discovery
    video_ids: list of video IDs

    Returns:
    Dataframe with video IDs and associated top level comment in text.

    """
    all_comments = []
    comments_list = []
    count = 0
    for video_id in video_ids:
        try:
            request = youtube.commentThreads().list(
                part="snippet,replies",
                videoId=video_id
            )
            response = request.execute()

            comments_in_video = [comment['snippet']['topLevelComment']['snippet']['textOriginal'] for comment in
                                 response['items'][0:10]]
            comments_in_video_info = {'video_id': video_id, 'comments': comments_in_video}

            all_comments.append(comments_in_video_info)
            #data = dict(response['items'][0:10]['comment']['snippet']['topLevelComment']['snippet']['id'])
            if len(response['items']) > 0:
                data = dict(Comment_id=response['items'][0]['snippet']['topLevelComment']['id'],
                            video_id=video_id,
                            comment_text= comments_in_video, #response['items'][0]['snippet']['topLevelComment']['snippet']['textOriginal'],
                            commets_author=response['items'][0]['snippet']['topLevelComment']['snippet'][
                                'authorDisplayName'],
                            comment_published_date=response['items'][0]['snippet']["topLevelComment"]['snippet'][
                                'publishedAt']
                            )
                comments_list.append(data)
                print(data)

        except:
            # When error occurs - most likely because comments are disabled on a video
            print('Could not get comments for video ' + video_id)

    return pd.DataFrame(all_comments),comments_list
    #print('Comments details:\n', data)


che_id = []

url ='https://www.youtube.com/watch?v=-sMsc7DKU1Y&list=PLKDs2UuoJx9Aiyv7n5gDPzMnh0OhRapvL'
video = YouTube(url)
ch_id = video.channel_id
#print('Chennal_id',ch_id)
che_id.append('UCQ4E5M87JHwwGhTx5VQ52VQ')

channel_data,chennal_dict = get_channel_stats(youtube, che_id)
#print(channel_data)
df = pd.DataFrame(channel_data)
#print(df.columns)
print(df[['channelName', 'subscribers', 'views', 'totalVideos']])

numeric_cols = ['subscribers', 'views', 'totalVideos']
channel_data[numeric_cols] = channel_data[numeric_cols].apply(pd.to_numeric, errors='coerce')
sns.set(rc={'figure.figsize':(10,8)})
ax = sns.barplot(x='channelName', y='subscribers', data=channel_data.sort_values('subscribers', ascending=False))
ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.0f}'.format(x/1000) + 'K'))
plot = ax.set_xticklabels(ax.get_xticklabels(),rotation = 90)
plt.show()

#video_df = pd.DataFrame()
#comments_df = pd.DataFrame()

def app():
    che_id = []
    st.markdown('''
        # **Web Application for Youtube Data Harvesting**

        This is the **Web App** created in Streamlit using the **Python** library.

        **Developed using :** App built in `Python` + `Streamlit` by [Vi.S.Senthilkumar](ML [Data Engineer ])

        ---
        ''')

    # Upload CSV data
    with st.sidebar.header('1. Upload your CSV data'):
        uploaded_file = st.sidebar.file_uploader("Upload your input CSV file", type=["csv"])
        st.sidebar.markdown("""
        [Example CSV input file](https://raw.githubusercontent.com/dataprofessor/data/master/delaney_solubility_with_descriptors.csv)
        """)
    st.sidebar.header('1. Input Your Youtube URL')
    video_url = st.sidebar.text_input('YouTube URL', '')


    # Display the provided URL
    st.write(f"Entered URL: {video_url}")

    # Check if the URL is not empty and the user clicks the "Download" button
    if st.sidebar.button("Submit"):
        # Download the video

            video = YouTube(video_url.strip())
            id = video.channel_id
            #st.write(ch_id)
            ch_id ='UCQ4E5M87JHwwGhTx5VQ52VQ'
            #video = YouTube(url)
            #ch_id = video.channel_id
            # print('Chennal_id',ch_id)
            che_id.append(id) #'UCQ4E5M87JHwwGhTx5VQ52VQ')

            channel_data,chennal_dict = get_channel_stats(youtube, che_id)
            # print(channel_data)
            df = pd.DataFrame(channel_data)
            # print(df.columns)
            print(df[['channelName', 'subscribers', 'views', 'totalVideos']])

            numeric_cols = ['subscribers', 'views', 'totalVideos']
            channel_data[numeric_cols] = channel_data[numeric_cols].apply(pd.to_numeric, errors='coerce')
            sns.set(rc={'figure.figsize': (10, 8)})
            ax = sns.barplot(x='channelName', y='subscribers', data=channel_data.sort_values('subscribers', ascending=False))
            ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.0f}'.format(x / 1000) + 'K'))
            plot = ax.set_xticklabels(ax.get_xticklabels(), rotation=90)

            for c in channel_data['channelName'].unique():
                    #print("Getting video information from channel: " + c)
                    playlist_id = channel_data.loc[channel_data['channelName'] == c, 'playlistId'].iloc[0]
                    video_ids,play_list = get_video_ids(youtube, playlist_id)

                    # get video data
                    video_data,video_dtls = get_video_details(youtube, video_ids)
                    video_df = pd.DataFrame(video_data)
                    # get comment data
                    comments_data,commnts_info = get_comments_in_videos(youtube, video_ids)
                    comments_df = pd.DataFrame(comments_data)
                    # append video data together and comment data toghether
                    video_df = video_df.append(video_data, ignore_index=True)

                    comments_df = comments_df.append(comments_data, ignore_index=True)
                    print(comments_df)
                    print(video_data)
            # Web App Title
            st.write(channel_data)
            st.write('Comments on Your Videos:')
            st.write(comments_df)
            st.write('Video Informations:')
            st.write(video_df)

            sns.set(rc={'figure.figsize': (10, 8)})
            ax = sns.barplot(x='channelName', y='subscribers', data=channel_data.sort_values('subscribers', ascending=False))
            ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.0f}'.format(x / 1000) + 'K'))
            plot = ax.set_xticklabels(ax.get_xticklabels(), rotation=90)
            st.bar_chart(x='channelName', y='subscribers', data=channel_data.sort_values('subscribers', ascending=False))

def Multi_channels():
    channel_id = []
    st.sidebar.title(' **Mutiple URL List**')
    st.markdown('''
            # **Web Application for Youtube Data Harvesting**

            This is the **Web App** created in Streamlit using the **Python** library.

            **Developed using :** App built in `Python` + `Streamlit` by [Vi.S.Senthilkumar](ML [Data Engineer ])

            ---
            ''')

    video_url = st.sidebar.text_input('Please enter Sinple or Mutiple YouTube URL comma separated:', '')
    st.sidebar.write(''' Example:
                     https://www.youtube.com/watch?v=X05Q2tzeQBk,
                     https://www.youtube.com/watch?v=-U48AAZo_Cw&t=12s
                     ''')

    with st.sidebar.header('1. Upload your channel url CSV data'):
        uploaded_file = st.sidebar.file_uploader("Upload your input channel url CSV file", type=["csv"])
        st.sidebar.markdown("""
        [Example CSV input file](https://raw.githubusercontent.com/dataprofessor/data/master/delaney_solubility_with_descriptors.csv)
        """)
    if uploaded_file is not None:
       url_df = pd.read_csv(uploaded_file)

       if url_df is not None:
          for i in url_df:
             id=YouTube(i)
             channel_id.append(id.channel_id.strip())
             st.write(channel_id)
    # Display the provided URL
    # st.write(f"Entered URL: {video_url}")

    # Check if the URL is not empty and the user clicks the "Download" button
    if st.sidebar.button("Submit"):
        #if len(channel_id) == 0:
        if video_url is not None:
            url_list = video_url.split(",")
            #st.write(url_list)
            if len(url_list) > 0:
                for i in url_list:
                    id = YouTube(i)
                    channel_id.append(id.channel_id.strip())
    #st.write(channel_id)
    if len(channel_id) > 0:
        channel_data,chennal_dict = get_channel_stats(youtube, channel_id)
        Mongo_db_Operations(chennal_dict,'channel')
        MySqlOperation(chennal_dict,'channel')
        # print(channel_data)
        df = pd.DataFrame(channel_data)
        # print(df.columns)
        print(df[['channelName', 'subscribers', 'views', 'totalVideos']])

        numeric_cols = ['subscribers', 'views', 'totalVideos']
        channel_data[numeric_cols] = channel_data[numeric_cols].apply(pd.to_numeric, errors='coerce')
        sns.set(rc={'figure.figsize': (10, 8)})
        ax = sns.barplot(x='channelName', y='subscribers', data=channel_data.sort_values('subscribers', ascending=False))
        ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.0f}'.format(x / 1000) + 'K'))
        plot = ax.set_xticklabels(ax.get_xticklabels(), rotation=90)

        for c in channel_data['channelName'].unique():
            # print("Getting video information from channel: " + c)
            playlist_id = channel_data.loc[channel_data['channelName'] == c, 'playlistId'].iloc[0]
            video_ids,play_list = get_video_ids(youtube, playlist_id)
            MySqlOperation(play_list, 'Playlist')
            # get video data
            video_data,video_dtls = get_video_details(youtube, video_ids)
            Mongo_db_Operations(play_list, 'Playlist')

            Mongo_db_Operations(video_dtls, 'video')
            video_df = pd.DataFrame(video_data)
            # get comment data
            comments_data, commnts_info = get_comments_in_videos(youtube, video_ids)
            Mongo_db_Operations(commnts_info, 'Comments')
            comments_df = pd.DataFrame(comments_data)
            # append video data together and comment data toghether
            video_df = video_df.append(video_data, ignore_index=True)
            #Mongo_db_Operations(video_data, 'Video')
            comments_df = comments_df.append(comments_data, ignore_index=True)
            print(comments_df)
            print(video_data)
        # Web App Title
        st.write(channel_data)
        st.write('Comments on Your Videos:')
        st.write(comments_df)
        st.write('Video Informations:')
        st.write(video_df)

        sns.set(rc={'figure.figsize': (10, 8)})
        ax = sns.barplot(x='channelName', y='subscribers', data=channel_data.sort_values('subscribers', ascending=False))
        ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.0f}'.format(x / 1000) + 'K'))
        plot = ax.set_xticklabels(ax.get_xticklabels(), rotation=90)
        st.bar_chart(x='channelName', y='subscribers', data=channel_data.sort_values('subscribers', ascending=False))

def Mongo_db_Operations(InputData,db):
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["Youtube"]
    print(mydb)
    print("display db names")
    print(myclient.list_database_names())
    print("create collection")
    mycol = mydb[db]
    #rec = mydb.mycol.insert_many(InputData)
    if db == 'channel' :
       rec = mydb.chennal.insert_many(InputData)

    if db =='Comments':
        rec = mydb.Comment.insert_many(InputData)
    if db == 'Playlist':
        rec = mydb.Playlist.insert_many(InputData)
    if db == 'video':
        rec = mydb.video.insert_many(InputData)

def MySqlOperation(data,db):
        import mysql.connector

        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="Viss@#123",
            database="mydb"

        )

        print(connection)
        mysql = connection.cursor()

        mysql.execute("SHOW tables")

        for db in mysql:
            print(db)
        if db == 'chennal':
            sql = "INSERT INTO channel (channelName ,chennai_Id,subscribers ,views,totalVideos,description,chennal_status,playlistId) VALUES (%s, %s,%s,%s,%s,%s,%s,%s)"
            for item in data:

                val = (item['channelName'], item['chennai_Id'], item['subscribers'], item['views'],
                       item['totalVideos'], item['description'],item['chennal_status'],item['playlistId'])
                mysql.execute(sql, val)

            connection.commit()
        if db == 'Playlist':
            sql = "INSERT INTO Playlist (Playlist_id,channel_id,Playlist_name) values (%s,%s,%s)"
            for item in data:
                val = (item['playlist_id'], item['channel_ID'],item['Playlistname'])
                mysql.execute(sql, val)
            connection.commit()

if __name__ == "__main__":
    #app()
    Multi_channels()
