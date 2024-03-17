from msilib.text import tables
import googleapiclient.discovery
import pymongo          # 1st vs to mongodb
import psycopg2         # 2nd vs to mongodb to postgres sql
import pandas as pd     # 3rd vs to mongodb to postgres sql create table frame
import streamlit as st
import webbrowser

def api_id():

    api_key = 'AIzaSyCN8s8FW1Ty1E9-rHM7JCRqOOCbYGVRM7I'
    import googleapiclient.discovery
    api_service_name = "youtube"
    api_version = "v3"
    youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)
    return youtube

youtube=api_id()


def channelinfo(channelid):

    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channelid
    )
    ch_namedata =request.execute()
    if 'items' in ch_namedata and len(ch_namedata['items']) > 0:
        ch_details = {
            'channel_name' : ch_namedata['items'][0]['snippet']['title'],
            'channel_id' : ch_namedata['items'][0]['id'],
            'channel_subscriberCount' : ch_namedata['items'][0]['statistics']['subscriberCount'],
            'channel_view' : ch_namedata['items'][0]['statistics']['viewCount'],
            'channel_video' : ch_namedata['items'][0]['statistics']['videoCount'],
            'channel_description' : ch_namedata['items'][0]['snippet']['description'],
            'channel_playlists' : ch_namedata['items'][0]['contentDetails']['relatedPlaylists']['uploads']}

        return ch_details
    else:
        return {'error': 'No channel data found'}
        

def collect_video_ids(channel_id):
    total_videos=[]
    request=youtube.channels().list(part="contentDetails",
                                    id=channel_id)
    palylistidresponse=request.execute()

    playid=palylistidresponse['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_Page_Token=None

    request1=youtube.playlistItems().list(part="snippet",
                                        playlistId=playid,
                                        maxResults=50,
                                        pageToken=next_Page_Token)
    videoidresponse=request1.execute()
    videoidresponse['items'][0]['snippet']['resourceId']['videoId']

    for i in range(len(videoidresponse['items'])):
        total_videos.append(videoidresponse['items'][i]['snippet']['resourceId']['videoId'])
    next_Page_Token=videoidresponse.get('nextPageToken')


    return total_videos        

#get video information
def collect_video_deatails(video_ids):
    vid_data=[]
    for videoid_data in video_ids:
        request3 = youtube.videos().list(part='snippet,contentDetails,statistics',
                                        id=videoid_data)
        videoresponse = request3.execute()

        for item in videoresponse['items']: # nested for loop code
            data_list=dict(channel_Name=item['snippet']['channelTitle'],
                        channel_Id=item['snippet']['channelId'],
                        video_Id=item['id'],
                        title=item['snippet']['title'],
                        video_description=item['snippet']['description'],
                        tags=item['snippet'].get('tags'),
                        published_at=item['snippet']['publishedAt'],
                        view_count=item['statistics']['viewCount'],
                        like_count=item['statistics'].get('likeCount',0),
                        favorite_count=item['statistics']['favoriteCount'],
                        comment_count=item['statistics'].get('commentCount',0),
                        duration=item['contentDetails']['duration'],
                        thumbnail=item['snippet']['thumbnails']['default']['url'],
                        caption_status=item['contentDetails']['caption'])
            vid_data.append(data_list)
    return vid_data


def collect_commentdetails(total_videos):
    commentdata_list = []

    from googleapiclient.errors import HttpError

    for commentid_data in total_videos:
        try:
            request4 = youtube.commentThreads().list(part='snippet',
                                                    videoId=commentid_data,
                                                    maxResults=50)
            commentresponse=request4.execute()
        except HttpError as e:
            if e.resp.status == 403:
                print(f"comments are disabled for video: {commentid_data}")
                continue

            else:
                raise

        for item in commentresponse['items']:comment_data = dict(
                                                        comment_Id=item['snippet']['topLevelComment']['id'],
                                                        video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                                                        comment_text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                                                        comment_author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                                        comment_published_date=item['snippet']['topLevelComment']['snippet']['publishedAt'])

        commentdata_list.append(comment_data)

    return commentdata_list


def collect_playlist_details(channel_id): 
    nextPageToken=None
    collect_playlistdetails=[]


    request5 = youtube.playlists().list(
                part = 'snippet,contentDetails',
                channelId = channel_id,
                maxResults=50,
                pageToken=nextPageToken)

    playlist_response = request5.execute()

    for item in playlist_response['items']:
        playlist_data=dict(channel_id=item['snippet']['channelId'],
                            playlist_id=item['id'],
                            playlist_name=item['snippet']['channelTitle'],
                            published_at=item['snippet']['publishedAt'])

        collect_playlistdetails.append(playlist_data)

    nextPageToken=playlist_response.get('nextPageToken')

    return collect_playlistdetails
    

# Assuming mdbclient is your MongoClient instance
mongodbcx=pymongo.MongoClient("mongodb+srv://shibu266277:admin123@shibu.oxmwtfe.mongodb.net/?retryWrites=true&w=majority&appName=SHIBU")
db=mongodbcx["YouTube1st_Project"]


def channel_data(channel_id):
    ytch_details=channelinfo(channel_id)
    ytpl_details=collect_playlist_details(channel_id)
    ytvid_ids=collect_video_ids(channel_id)
    ytvid_details=collect_video_deatails(ytvid_ids)
    comm_details=collect_commentdetails(ytvid_ids)

    collect=db["channel_details"]
    collect.insert_one({"channel_collection":ytch_details,"playlist_collection":ytpl_details,"video_collection":ytvid_details,
                        "command_collection":comm_details})

    return "data_files insert successful"


# collect data from mongodb to data frame, then insert to  postgres sql

def channel_table():
    mdb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="12345",
                        database="project_youtube",
                        port="5432")
    cursor=mdb.cursor()

    drop_query='''drop table if exists channels'''
    cursor.execute(drop_query)
    mdb.commit()

    try:
        create_query='''create table if not exists channels(channel_name varchar(255),
                                                        channel_id varchar(255) primary key,
                                                        channel_subscriberCount bigint,
                                                        channel_view bigint,
                                                        channel_description text,
                                                        channel_playlists varchar(255),
                                                        channel_total_video bigint)'''
        cursor.execute(create_query)
        mdb.commit()

    except:
        print("Error creating channel_detail table")

    db = mongodbcx["YouTube1st_Project"]
    collect = db["channel_details"]

    ch_db_list = []
    for ch_db in collect.find({}, {"_id": 0, "channel_collection": 1}):
        ch_db_list.append(ch_db["channel_collection"])

    df = pd.DataFrame(ch_db_list)

    # insert data mongodb to postgres sql for table methods.
    for index,row in df.iterrows():
        insert_query='''insert into channels(
                                            channel_name,
                                            channel_id,
                                            channel_subscriberCount,
                                            channel_view,
                                            channel_total_video,
                                            channel_description,
                                            channel_playlists)
                                            values(%s,%s,%s,%s,%s,%s,%s)'''

        val=(
            row['channel_name'],
            row['channel_id'],
            row['channel_subscriberCount'],
            row['channel_view'],
            row['channel_total_video'],
            row['channel_description'],
            row['channel_playlists'])

        try:
            cursor.execute(insert_query,val)
            mdb.commit()

            print("data inserted successfully!")

        except Exception as e:
            print(f"Error: {e}")
            mdb.rollback()  

#get playlists_collection from mongodb

def playlist_table():
    mdb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="12345",
                        database="project_youtube",
                        port="5432")
    cursor=mdb.cursor()

    drop_query='''drop table if exists playlists'''
    cursor.execute(drop_query)
    mdb.commit()


    create_query='''create table if not exists playlists(
                                                        playlist_id varchar(255)primary key,
                                                        channel_id varchar(255),
                                                        playlist_name varchar(255))'''
    cursor.execute(create_query)
    mdb.commit()

    db=mongodbcx["YouTube1st_Project"]
    collect=db["channel_details"]

    pl_db_list=[]
    for pl_db in collect.find({},{"_id":0,"playlist_collection":1}):
        for i in range(len(pl_db["playlist_collection"])):
            pl_db_list.append(pl_db["playlist_collection"][i]) 
    df1=pd.DataFrame(pl_db_list)

    for index,row in df1.iterrows():
        insert_query='''insert into playlists(
                                            playlist_id,
                                            channel_id,
                                            playlist_name)
                                            values(%s,%s,%s)'''

        val=(
            row['playlist_id'],
            row['channel_id'],
            row['playlist_name'])


        cursor.execute(insert_query,val)
        mdb.commit()



def video_table():
    mdb=psycopg2.connect(host="localhost",
                            user="postgres",
                            password="12345",
                            database="project_youtube",
                            port="5432")
    cursor=mdb.cursor()

    drop_query='''drop table if exists videos'''
    cursor.execute(drop_query)
    mdb.commit()


    create_query='''create table if not exists videos(channel_Name varchar(255),
                                                    channel_Id varchar(255),
                                                    video_Id varchar(255) primary key,
                                                    title varchar(255),
                                                    video_description text,
                                                    tags text,
                                                    published_at timestamp,
                                                    view_count int,
                                                    like_count int,
                                                    favorite_count int,
                                                    comment_count int,
                                                    duration interval,
                                                    thumbnail varchar(255),
                                                    caption_status varchar(255))'''
    cursor.execute(create_query)
    mdb.commit()

    db=mongodbcx["YouTube1st_Project"]
    collect=db["channel_details"]

    vi_db_list=[]
    for vi_db in collect.find({},{"_id":0,"video_collection":1}):
        for i in range(len(vi_db["video_collection"])):
            vi_db_list.append(vi_db["video_collection"][i]) 
    df2=pd.DataFrame(vi_db_list)

    for index,row in df2.iterrows():
        insert_query='''insert into videos(
                                    channel_Name,
                                    channel_Id,
                                    video_Id,
                                    title,
                                    video_description,
                                    tags,
                                    published_at,
                                    view_count,
                                    like_count,
                                    favorite_count,
                                    comment_count,
                                    duration,
                                    thumbnail,
                                    caption_status)
                                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''

        val=(
                row['channel_Name'],
                row['channel_Id'],
                row['video_Id'],
                row['title'],
                row['video_description'],
                row['tags'],
                row['published_at'],
                row['view_count'],
                row['like_count'],
                row['favorite_count'],
                row['comment_count'],
                row['duration'],
                row['thumbnail'],
                row['caption_status'])


        cursor.execute(insert_query,val)
        mdb.commit()


def comment_table():

    mdb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="12345",
                        database="project_youtube",
                        port="5432")
    cursor=mdb.cursor()

    drop_query='''drop table if exists comments'''
    cursor.execute(drop_query)
    mdb.commit()

    create_query='''create table if not exists comments(
                                                    comment_Id varchar(255) primary key,
                                                    video_Id varchar(255),
                                                    comment_text text,
                                                    comment_author varchar(255),
                                                    comment_published_date timestamp)'''
    cursor.execute(create_query)
    mdb.commit()

    mdb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="12345",
                        database="project_youtube",
                        port="5432")
    cursor=mdb.cursor()


    db=mongodbcx["YouTube1st_Project"]
    collect=db["channel_details"]

    com_db_list=[]
    for com_db in collect.find({},{"_id":0,"command_collection":1}):
        for i in range(len(com_db["command_collection"])):
            com_db_list.append(com_db["command_collection"][i])
    df3=pd.DataFrame(com_db_list)


    for index,row in df3.iterrows():
            insert_query='''insert into comments(
                                                comment_Id,
                                                video_Id,
                                                comment_text,
                                                comment_author,
                                                comment_published_date)
                                                values(%s,%s,%s,%s,%s)'''

            val=(
                row['comment_Id'],
                row['video_Id'],
                row['comment_text'],
                row['comment_author'],
                row['comment_published_date'])

            try:
                cursor.execute(insert_query, val)
                mdb.commit()
                print("Data inserted successfully!")
            except psycopg2.IntegrityError as e:
                # Handle the case where the comment_Id already exists (duplicate key)
                mdb.rollback()
                print(f"Skipped duplicate record: {e}")
            except Exception as e:
                # Handle other exceptions
                mdb.rollback()
                print(f"Error: {e}")  



def all_tables():
    channel_table()
    playlist_table()
    video_table()
    comment_table()

    return "four tables insert done!!!"

def channel_tables_view():
    ch_db_list = []
    db = mongodbcx["YouTube1st_Project"]
    collect = db["channel_details"]
    for ch_db in collect.find({}, {"_id": 0, "channel_collection": 1}):
        ch_db_list.append(ch_db["channel_collection"])
    df = st.dataframe(ch_db_list)

    return df

def playlist_tables_view():
    pl_db_list=[]
    db=mongodbcx["YouTube1st_Project"]
    collect=db["channel_details"]
    for pl_db in collect.find({},{"_id":0,"playlist_collection":1}):
        for i in range(len(pl_db["playlist_collection"])):
            pl_db_list.append(pl_db["playlist_collection"][i]) 
    df1=st.dataframe(pl_db_list)

    return df1

def video_tables_view():
    vi_db_list=[]
    db=mongodbcx["YouTube1st_Project"]
    collect=db["channel_details"]
    for vi_db in collect.find({},{"_id":0,"video_collection":1}):
        for i in range(len(vi_db["video_collection"])):
            vi_db_list.append(vi_db["video_collection"][i]) 
    df2=st.dataframe(vi_db_list)

    return df2

def comment_tables_view():
    com_db_list=[]
    db=mongodbcx["YouTube1st_Project"]
    collect=db["channel_details"]
    for com_db in collect.find({},{"_id":0,"command_collection":1}):
        for i in range(len(com_db["command_collection"])):
            com_db_list.append(com_db["command_collection"][i])
    df3=st.dataframe(com_db_list)

    return df3


# streamlit application code

# Function to open YouTube webpage
def open_youtube():
    webbrowser.open_new_tab("https://www.youtube.com/")
st.sidebar.header(":blue[🎉Youtube Data Harvesting And Warehousing🎉]")

# YouTube logo image.svg
youtube_logo_url = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAOEAAADgCAMAAADCMfHtAAAAolBMVEX/////AACSkpLAwMDx8fGZmZnPz8+UlJT/7e2QkJD/dnb/RET/NzfLy8vr6+v09PT/kpKmpqa2tragoKDZ2dn/W1uvr6/h4eG9vb2urq7/n5//mpr/9fXe3t7T09P/1tb/ycn/vLz/sLD/p6f/bGz/h4f/5OT/UVH/Skr/e3v/0dH/Ghr/3t7/jY3/trb/goL/JSX/ZGT/Vlb/MDD/EhL/w8PCLtHcAAAImklEQVR4nO2dZ3vaPBuGeWSEMMR2PMA4bMiCJmT07f//a6+MQ5lGsnUrGs31pT2SlOqMpHtpNRrcCvzEG0RZGGN1isMsGniJH/A3m1O+18dIJ+G+58PhjSOCCEEozpzJOPX9pjr5fjqeOFmM8gaRaAyB5w/oRyESeinEp4Ep9cJtuwaiPTkO3fxj9KLbKc1/+W4o0pEJpoMh0hOvUBrR0Rondf81He24BdogGWpRExjX6YUgQwRPwNsjQxNMUFbZf7Ro73symiNFHp1N1XqDdiCK4L2qPAV9VKkbU0JI3dmrSgltM/ds9FwSmtSBhYKQuJzzKkLIkdsYSXLo1OL5ORormDZCd0po7MX+qbjCcNZO1IDErJ+hgM3vaIskNZmIMcHm2ZhDBfj6QA0JMRuQIhKSlX+XhrGAaaUi+TRZKPteC7nmGpm9UoRKIjjfLfuOYZog9/JQxOW9a5gigi9/melKjFF8qbNSF5nsCI/VvGRRMNI/nedX63ycOjwRnUGKyUn2EJSZH1NFXcZx7BJZY0d3yo6JmqfE5osiHVrOiAyUNUWW+oedSGehbV24DdH2UI51szBXdmBOCbIh4j5VisjurwmyJ147FP5bcMpOvaMlcna5cEBceyLSQzXdr4pFguwK2PYKv4ZpZNASTDV5XxViguwKSffyC2vq742qdSo6b4KuVN8MV7atPA2snYb5RMzj7dDKgKbQeOsmsEX1mVM1Ec7zCnsNDTU11Of7lgalhWJqTMeor7oZEpWhMY3ZbMwNd4po3DZB9hUw9hpQh+gZui2BTw519o7FDr+x7T/Hqmr+qVo/hMbrh9B8AROO2o8v0/VsPp8vlnfD4e3t7euq0+nc954+e73ec/dQ+Vd6vQ/67c7qlf7kcDhcLpeb2Z/Zej19eWy3RyBNEiCkNLP54u51df/x9Pt//0nS+03vfvU6XG7m63rI9QhH84fnd1lMV/TrZrVofwPhoqsAbq+bpWTCO6V4haowViVcq4Yr9D6VRXirGu2v7uQQ3qvmOtBKBmFPNdWR7uEJV6qZTnQLTThXTXSmNTChap5zvcMSDlXzXNAClFA1zSX9giRcqKa5qD+AhGpj0TL14AgfVbOUiJ1Q8RLqEG9fEtvW8BLqOUh5hikvoWqSUkERzlSDlIqZRnES6pM1nYqZRXESPqkGKRUzw+AklFZKE9YNDOFINccVsTwiH6Gu/j4Xq7rIR6ivKWUniXyEc9UYVzQHIdQ1ZsvFqp3yEb6pxriiVxBCnaqIp+qAED6rxrgiVuzNRyi2zjSVOgRYLp+PUKwN7caLzKhPC8JGYypvoEMQCgZtRdQx+w0DdCZG2MZFKBi07eKquZxlY0bYxkX4AtWEhYwc5QWAcCrWhMNf8hKG6lAQhIKB9/EwAl8dYITeXIRzsSacTJTRKwTXXoy6NxehYEX/zBSMQOPcDQCh4OS5YOzagGEOI7ngIhRMni6a88cPGD5mtU0ZIbXQQKEcBKGg+St1yWuQtQIIQsF68JWgAyKUY2xY4CJ8kEZIPdEvUUJGks9FKGjcGYGj6OryGwBhRyqhqCVjbI7SglBspjMKNZoQNkb157ohhJSx7v9iDCENc+qFcgYR1jTaBhFu6rlGYwhru35DCGc3tT/eCEKhENwAQsE0SnvCF9Hd45oTAqT6EISCW9jLCduCv7utICJvSdlTG2bzP0T2JCUDFoi1j/UAQCijigG3Uw6iigFfiYLc3DEEIISuJsIuz+hXL90AL7FBEEJW9cVra6eCqOrDrcz8kbDUzdjM/q2razA17lMxNrZ94wqprO0YMwBCiFVu4QC7VIy97N+0UwFuLe1cEOv4grtNRiABdqkeAQgFdwxJ3tkIsWNI3xMzuRht/yEsBB6HAIp1GJiPUNeTa7meQQh13gXNOhZk/k52RhGDk1DHk+o7sQ6v/ZwoKaTzqaAZCKHOJ7sYQdvP6byd9D1hybw4wvhTsp9AhPqedGZUSy04rT4DItTX1LBabvytEU9ghLqeImXfvmf67S3sXRCG38DDSg6rEOp5ixIr7K5CqGethqPdZt9mxnM9pNE30vFcZlaFcKMa6EwzYMKGxLWHWmJVaKoTanYsn5lV1CBsfKqmOhDHZXQ1CBvAhyMFxHsFbeWbkqeyzpxXFIerr0lITaqKq8pPxNjoJUjYaKzVXrf7+67S7fM179V/WTx0VfTl786CVT0EIiz0ONssbx86H0/dd4kvBzx93K9ev/vlgBKNiscf1vnzD5vlMn//IX8A4vZt+wAE1Wf35kzdbvf5qfdx33l7yF+AuFsu5rP1NH/+QfnrD4boh9B8/RCar4LQ9lfJ7H9ZrmX964D2v/Bo9yudfTT+B15atfy1XBT8Ay8e/wOvVtv/8rj9r8f7FpsaQk3p/g8Lteu8yNqJ6H3Fa4m1Pj+mMVuugFjqEZuIBMXfMmJnAuWQnZdItp7fPuGvQdrIramNYU164AYdYmMGdTj5msi1z9b47qEB7RP7Ev3oaGD6yDqH0UTHoVrfupmYnRDRmWhXcJqejUqH2BW6xedRDCY2xd8eOQ9ixjYZG2pmxudf7Vs0TuPLhhNb4xSjC2M0V+puqxrma1LqGDxkRQROHUVpT0WEmO8VfUKurKaFZJcVG6uAkKvV0ZhgsxEDTMLrPxETYrJbbBK2z6OI5pqblAMwn4v78oZhShBriBbqE0OX9geINwd0XBKaZ2+CmLjcVdExMW+k0hFKLkTbZQroZOyb1I1BH1Uddx4iBm0lovOq+vJSM0QEm7HlrUUnVVjHi49jyqh/4u9hguIKM/BICabTN9I5AEhpsoCwiFUch3SEY0fPjMN3aA+4Yd3++/sxA4wQIpmnF6XvZYS2Cw9AmpVGFJJ+XpwNWpMk9Zvq5KfJpDXI4m17MOQE8lv9vCv1Ee634AdV4CfeIMrCGKtTHGbRwEv8Cs79/8XdtyVR1B2FAAAAAElFTkSuQmCC"
st.sidebar.image(youtube_logo_url, width=50)



# Button to open YouTube webpage
if st.sidebar.button("Open YouTube"):
    open_youtube()
    
# Streamlit application code
with st.sidebar:
    
    st.balloons()
    channel_id = st.text_input(":blue[ENTER CHANNEL ID]")
    st.subheader(":orange[💻SKILL EFFORT GIVEN]") 
    st.caption(":blue[Data Infromation]")
    st.caption(":blue[Python Coding]")
    st.caption(":blue[MonogoDB Data Lake]")
    st.caption(":blue[Data Tranfer Using by Api Key]")
    st.caption(":blue[Channels Data Using MongoDB With PostGres SQL]")
    st.caption(":blue[Data Information Present With Streamlit]") 
    
       

if st.button("📊 COLLECT DATA TO DATALAKE"):
    ch_idlist=[]
    db=mongodbcx["YouTube1st_Project"]
    collect=db["channel_details"]
    for ch_db in collect.find({},{"_id": 0,"channel_collection":1}):
        ch_idlist.append(ch_db['channel_collection']['channel_id'])

    if channel_id in ch_idlist:
        st.write("GIVEN CHANNEL ID IS ALREADY EXISTS!!!")

    else:
        insert = channel_data(channel_id)
        st.success(insert)

if st.button("📊 CREATE POSTGRES SQL"):
    result = all_tables()
    st.success(result)


selected_channel = st.selectbox("Choose A Channel", 
                                        ("RIZWANA'S KITCHEN", "RIHANNA", "BILLIE EILISH", 
                                         "JUST LAUGH", "RITHU ROCK", "INDIAN ACTORS DATA", 
                                         "HOW TO YOUTUBE", "SPJ KIDS TV", "UNITY DECK", 
                                         "KIDS ROMI WORLD"))


show_table=st.radio("CHOOSE THE TABLE FOR VIEW",("CHANNEL","PLAYLIST","VIDEO","COMMENT"))

if show_table=="CHANNEL":
    channel_tables_view()

elif show_table=="PLAYLIST":
    playlist_tables_view()

elif show_table=="VIDEO":
    video_tables_view()

elif show_table=="COMMENT":
    comment_tables_view()


# sql conect to streamlit

mdb=psycopg2.connect(host="localhost",
                    user="postgres",
                    password="12345",
                    database="project_youtube",
                    port="5432")
cursor=mdb.cursor()

Question=st.selectbox("CHOOSE THE QUIZ QUESTIONS",("1.What are the names of all the videos and their corresponding channels?",
                                              "2.Which channels have the most number of videos, and how many videos do they have?",
                                              "3.What are the top 10 most viewed videos and their respective channels?",
                                              "4.How many comments were made on each video, and what are their corresponding video names?",
                                              "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
                                              "6.What is the total number of likes for each video, and what are their corresponding video names?",
                                              "7.What is the total number of views for each channel, and what are their corresponding channel names?",
                                              "8.What are the names of all the channels that have published videos in the year 2022?",
                                              "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                              "10.Which videos have the highest number of comments, and what are their corresponding channel names?"))

if Question=="1.What are the names of all the videos and their corresponding channels?":
    ans1_query = '''Select title AS "Videos", channel_name AS "Channel Name" from videos'''
    cursor.execute(ans1_query)
    mdb.commit()
    tab1 = cursor.fetchall()
    df1 = pd.DataFrame(tab1, columns=["Videos", "Channel Names"])
    st.write(df1)

if Question=="2.Which channels have the most number of videos, and how many videos do they have?":
    ans2_query = '''Select channel_name AS "Channel Name",channel_total_video AS "No Of Videos" from channels order by channel_total_video desc'''
    cursor.execute(ans2_query)
    mdb.commit()
    tab2=cursor.fetchall()
    df2=pd.DataFrame(tab2, columns=["Channel Names", "No of Counts"])
    st.write(df2)

if Question=="3.What are the top 10 most viewed videos and their respective channels?":
    ans3_query = '''Select channel_name AS "Channel Names",video_id AS "Video IDS",view_count AS "View Counts" from videos order by view_count desc limit 10;'''
    cursor.execute(ans3_query)
    mdb.commit()
    tab3=cursor.fetchall()
    df3=pd.DataFrame(tab3, columns=["Channel Names", "Video IDS", "View Counts"])
    st.write(df3)

if Question=="4.How many comments were made on each video, and what are their corresponding video names?":
    ans4_query = '''Select title AS "Names Of Video Title",comment_count AS "No Of Comments" from videos'''
    cursor.execute(ans4_query)
    mdb.commit()
    tab4=cursor.fetchall()
    df4=pd.DataFrame(tab4, columns=["Names Of Videos Title", "No Of Comments"])
    st.write(df4)

if Question=="5.Which videos have the highest number of likes, and what are their corresponding channel names?":
    ans5_query = '''Select channel_name AS "Channel Names",like_count AS "No Of Likes" from videos order by like_count desc'''
    cursor.execute(ans5_query)
    mdb.commit()
    tab5=cursor.fetchall()
    df5=pd.DataFrame(tab5, columns=["Channel Names", "No Of Likes"])
    st.write(df5)

if Question=="6.What is the total number of like for each video, and what are their corresponding video names?":
    ans6_query = '''Select title AS "Video Names",like_count AS "Total No Of Likes" from videos'''
    cursor.execute(ans6_query)
    mdb.commit()
    tab6=cursor.fetchall()
    df6=pd.DataFrame(tab6, columns=["Video Names", "Total No Of Likes"])
    st.write(df6)

if Question=="7.What is the total number of views for each channel, and what are their corresponding channel names?":
    ans7_query = '''Select channel_name AS "Channel Names",channel_view AS "Total No Of Channel Views" from channels'''
    cursor.execute(ans7_query)
    mdb.commit()
    tab7=cursor.fetchall()
    df7=pd.DataFrame(tab7, columns=["Channel Names", "Total No Of Channel Views"])
    st.write(df7)

if Question=="8.What are the names of all the channels that have published videos in the year 2022?":
    ans8_query = '''SELECT channel_name AS "Channel_Names",published_at AS "Years" FROM videos WHERE EXTRACT(YEAR FROM published_at) = 2022;'''
    cursor.execute(ans8_query)
    mdb.commit()
    tab8=cursor.fetchall()
    df8=pd.DataFrame(tab8, columns=["Channel_Names", "Years"])
    st.write(df8)

if Question=="9.What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    ans9_query = '''SELECT channel_name AS "Channel Names", AVG(duration) AS "Average Duration" FROM videos GROUP BY channel_name;'''
    cursor.execute(ans9_query)
    mdb.commit()
    tab9=cursor.fetchall()
    df9=pd.DataFrame(tab9, columns=["Channel Names", "Average Duration"])
    st.write(df9)

if Question=="10.Which videos have the highest number of comments, and what are their corresponding channel names?":
    ans10_query = '''SELECT channel_name AS "Channel Names",title AS "Video Title Names",comment_count AS "No.Of Comments" FROM videos order by comment_count desc'''
    cursor.execute(ans10_query)
    mdb.commit()
    tab10=cursor.fetchall()
    df10=pd.DataFrame(tab10, columns=["Channel Names", "Video Names", "No.Of Highest Comments"])
    st.write(df10)

st.slider(":red[RATING FOR MY PROJECT]", 0, 100)
st.select_slider(":green[FEEDBACK]",["IMPORVE","SUPER","EXCELLENT"])
