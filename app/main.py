### System ###
import os
import re
import asyncio
import textwrap
import datetime as dt
from collections import defaultdict
from urllib.parse import urljoin, urlparse

### FastAPI ###
from typing import *
from pydantic import BaseModel, BaseSettings
from fastapi import FastAPI, BackgroundTasks
from fastapi_utils.tasks import repeat_every

### Security ###
# from jose import JWTError, jwt
# from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm

### Connectivity ###
import aiohttp

### Parsing ###
import arrow
from bs4 import BeautifulSoup

### Database ###
from pony.orm import (
    Database,
    PrimaryKey,
    Required,
    db_session,
    Optional as dOptional,
    Set as dSet,
)


class Settings(BaseSettings):
    DB_TYPE: str = "postgres"
    DB_FILE: str = None
    DATABASE_URL: str = None
    BASE_URL: str = "https://tfgames.site"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


config = Settings()


app = FastAPI(
    title="TFGS API",
    description=textwrap.dedent(
        """
    An unofficial REST API for TFGamesSite.
    This is currently a read-only API as you probably do not want to \
    share your credentals with an unknown third party.

    **This API is still a work in progress, so endpoints and output formats \
    are subject to change.**
    """
    ),
    version="1.0.0",
    openapi_tags=[
        {
            "name": "games",
            "description": "Operations on game entries in the database.",
        },
        {
            "name": "reviews",
            "description": "Operations on game reviews.",
        },
    ],
    redoc_url=None,
)


### Database ###


db = Database()


class Game(db.Entity):
    id = PrimaryKey(int, auto=True)
    title = Required(str)
    engine = Required("GameEngine")
    content_rating = Required("ContentRating")
    language = Required(str)
    release_date = Required(dt.datetime)
    last_update = Required(dt.datetime)
    version = Required(str)
    development_stage = Required(str)
    likes = Required(int)
    contest = dOptional(str)
    orig_pc_gender = dOptional(str)
    adult_themes = dSet("AdultTheme")
    transformation_themes = dSet("TransformationTheme")
    multimedia_themes = dSet("MultimediaTheme")
    thread = dOptional(str)
    play_online = dOptional(str)
    synopsis_text = dOptional(str)
    synopsis_html = dOptional(str)
    plot_text = dOptional(str)
    plot_html = dOptional(str)
    characters_text = dOptional(str)
    characters_html = dOptional(str)
    walkthrough_text = dOptional(str)
    walkthrough_html = dOptional(str)
    changelog_text = dOptional(str)
    changelog_html = dOptional(str)
    authors = dSet("GameAuthor")
    versions = dSet("GameVersion")
    reviews = dSet("Review")


class GameEngine(db.Entity):
    id = PrimaryKey(int, auto=True)
    name = Required(str)
    games = dSet(Game)


class ContentRating(db.Entity):
    id = PrimaryKey(int, auto=True)
    name = Required(str)
    games = dSet(Game)


class AdultTheme(db.Entity):
    id = PrimaryKey(int, auto=True)
    name = Required(str)
    games = dSet(Game)


class TransformationTheme(db.Entity):
    id = PrimaryKey(int, auto=True)
    name = Required(str)
    games = dSet(Game)


class MultimediaTheme(db.Entity):
    id = PrimaryKey(int, auto=True)
    name = Required(str)
    games = dSet(Game)


class GameAuthor(db.Entity):
    id = PrimaryKey(int, auto=True)
    name = Required(str)
    games = dSet(Game)


class GameDownload(db.Entity):
    id = PrimaryKey(int, auto=True)
    link = Required(str)
    report = Required(str)
    note = dOptional(str)
    delete = dOptional(str)
    game_version = Required("GameVersion")


class GameVersion(db.Entity):
    id = PrimaryKey(int, auto=True)
    version = Required(str)
    downloads = dSet(GameDownload)
    game = Required(Game)


class Review(db.Entity):
    id = PrimaryKey(int, auto=True)
    author = Required(str)
    text = Required(str)
    date = Required(dt.datetime)
    version = Required(str)
    game = Required(Game)


if config.DB_TYPE == "sqlite":
    db.bind(provider="sqlite", filename=os.path.abspath(config.DB_FILE), create_db=True)
else:
    parsed = urlparse(config.DATABASE_URL)
    db.bind(
        provider="postgres",
        user=parsed.username,
        password=parsed.password,
        host=parsed.hostname,
        port=parsed.port,
        database=parsed.path.lstrip("/"),
    )

db.generate_mapping(create_tables=True)


### Models ###


class PReview(BaseModel):
    id: int
    author: str
    version: str
    date: dt.datetime
    text: str


class PGame(BaseModel):
    id: int
    title: str
    authors: Dict[str, int]
    game_engine: str
    content_rating: str
    language: str
    release_date: dt.datetime
    last_update: dt.datetime
    version: str
    development_stage: str
    likes: int
    reviews: Optional[List[PReview]]
    contest: Optional[str]
    orig_pc_gender: Optional[str]
    themes: Optional[dict]
    thread: Optional[str]
    play_online: Optional[str]
    versions: Optional[dict]
    synopsis: Optional[Dict[str, str]]
    plot: Optional[Dict[str, str]]
    characters: Optional[Dict[str, str]]
    walkthrough: Optional[Dict[str, str]]
    changelog: Optional[Dict[str, str]]


class PGameReduced(BaseModel):
    id: int
    title: str
    authors: Dict[str, int]
    game_engine: str
    content_rating: str
    language: str
    release_date: dt.datetime
    last_update: dt.datetime
    version: str
    development_stage: str
    likes: int
    contest: Optional[str]
    orig_pc_gender: Optional[str]
    themes: Optional[dict]
    thread: Optional[str]
    play_online: Optional[str]
    versions: Optional[dict]
    synopsis: Optional[Dict[str, str]]
    plot: Optional[Dict[str, str]]
    characters: Optional[Dict[str, str]]
    walkthrough: Optional[Dict[str, str]]
    changelog: Optional[Dict[str, str]]


class PGameSearchResult(BaseModel):
    id: int
    title: str
    authors: Dict[str, int]
    game_engine: str
    content_rating: str
    language: str
    release_date: dt.datetime
    last_update: dt.datetime
    version: str
    development_stage: str
    likes: int
    contest: Optional[str]
    orig_pc_gender: Optional[str]
    themes: Optional[dict]
    thread: Optional[str]
    play_online: Optional[str]


class Topic(BaseModel):
    title: str
    last_author: str
    last_post_time: dt.datetime


class User(BaseModel):
    username: str
    groups: List[str]
    joined: dt.datetime
    warnings: int
    total_posts: int


class TFGSAuthInfo(BaseModel):
    __cfduid: str
    phpbb3_tfgs_u: str
    phpbb3_tfgs_sid: str
    phpbb3_tfgs_k: Optional[str]


class UserWithTFGSAuthInfo(User, TFGSAuthInfo):
    pass


class Token(BaseModel):
    access_token: str
    token_type: str


### Events ###


@app.on_event("startup")
@repeat_every(seconds=60 * 60)  # 1 hour
async def trigger_crawl_tfgs():
    print("Running scheduled crawl task")
    await crawl_tfgs()


@app.on_event("startup")
@repeat_every(seconds=60 * 60)  # 1 hour
async def trigger_crawl_tfgs():
    print("Running scheduled crawl task")
    await crawl_tfgs()


# @app.on_event("shutdown")
# def shutdown_event():
#     pass


### Utility Methods ###


def db_game_to_pgame(game):
    themes = {}
    themes["adult"] = {theme.name: theme.id for theme in game.adult_themes}
    themes["transformation"] = {
        theme.name: theme.id for theme in game.transformation_themes
    }
    themes["multimedia"] = {theme.name: theme.id for theme in game.multimedia_themes}

    versions = defaultdict(list)
    for version in game.versions:
        for download in version.downloads:
            versions[version.version].append(download.to_dict(exclude="game_version"))

    reviews = []
    for review in game.reviews:
        reviews.append(
            PReview(
                id=review.id,
                author=review.author,
                version=review.version,
                date=review.date,
                text=review.text,
            )
        )

    return PGame(
        id=game.id,
        title=game.title,
        authors={author.name: author.id for author in game.authors},
        version=game.version,
        game_engine=game.engine.name,
        content_rating=game.content_rating.name,
        language=game.language,
        release_date=game.release_date,
        last_update=game.last_update,
        development_stage=game.development_stage,
        likes=game.likes,
        contest=game.contest,
        orig_pc_gender=game.orig_pc_gender,
        themes=themes,
        thread=game.thread,
        versions=dict(versions),
        synopsis={"text": game.synopsis_text, "html": game.synopsis_html},
        plot={"text": game.plot_text, "html": game.plot_html},
        characters={"text": game.characters_text, "html": game.characters_html},
        walkthrough={"text": game.walkthrough_text, "html": game.walkthrough_html},
        changelog={"text": game.changelog_text, "html": game.changelog_html},
        reviews=reviews,
    )


def db_review_to_preview(review):
    return PReview(
        id=review.id,
        author=review.author,
        version=review.version,
        date=review.date,
        text=review.text,
    )


def pgame_to_db_game(game):
    db_game = Game.get(id=game.id)

    if db_game:
        return db_game

    for k, v in game.authors.items():
        author = GameAuthor.get(name=k, id=v)
        if not author:
            GameAuthor(id=v, name=k)

    db_game = Game(
        id=game.id,
        title=game.title,
        authors=[GameAuthor.get(id=v) for v in game.authors.values()],
        version=game.version or "1.0.0",
        engine=GameEngine.get(name=game.game_engine),
        content_rating=ContentRating.get(name=game.content_rating),
        language=game.language,
        release_date=game.release_date,
        last_update=game.last_update,
        development_stage=game.development_stage,
        likes=game.likes,
        contest=game.contest or "",
        orig_pc_gender=game.orig_pc_gender,
        adult_themes=[AdultTheme.get(id=v) for v in game.themes["adult"].values()]
        if game.themes
        else [],
        transformation_themes=[
            TransformationTheme.get(id=v)
            for v in game.themes["transformation"].values()
        ]
        if game.themes
        else [],
        multimedia_themes=[
            MultimediaTheme.get(id=v) for v in game.themes["multimedia"].values()
        ]
        if game.themes
        else [],
        thread=game.thread or "",
        play_online=game.play_online or "",
        synopsis_text=game.synopsis["text"] if game.synopsis else "",
        synopsis_html=game.synopsis["html"] if game.synopsis else "",
        plot_text=game.plot["text"] if game.plot else "",
        plot_html=game.plot["html"] if game.plot else "",
        characters_text=game.characters["text"] if game.characters else "",
        characters_html=game.characters["html"] if game.characters else "",
        walkthrough_text=game.walkthrough["text"] if game.walkthrough else "",
        walkthrough_html=game.walkthrough["html"] if game.walkthrough else "",
        changelog_text=game.changelog["text"] if game.changelog else "",
        changelog_html=game.changelog["html"] if game.changelog else "",
    )

    db_versions = []
    for version, downloads in game.versions.items():
        db_downloads = []

        version = GameVersion(version=version, game=db_game)
        for download in downloads:
            db_downloads.append(
                GameDownload(
                    delete=download["delete"] or "",
                    link=download["link"],
                    note=download["note"] or "",
                    report=download["report"],
                    game_version=version,
                )
            )
        version.downloads = db_downloads
        db_versions.append(version)

    db_game.versions = db_versions

    reviews = []
    for review in game.reviews:
        reviews.append(
            Review(
                author=review.author,
                text=review.text,
                date=review.date,
                version=review.version,
                game=db_game,
            )
        )

    db_game.reviews = reviews

    return db_game


### Public Routes ###


async def parse_category(html, name, db_cls):
    soup = BeautifulSoup(html, "lxml")

    objects = []
    for item in soup.find_all("div", class_="browsecontainer"):
        data = item.find("a")
        _name = data.text
        _link = f'"https://tfgames.site/{data["href"]}'
        _id = int(_link.split(f"{name}=")[1])
        _repr = _name.lower().replace(" ", "_")
        objects.append(db_cls(id=_id, name=_repr))

    return objects


async def fetch_category(session, name, db_cls):
    async with session.get(
        f"https://tfgames.site/?module=browse&by={name}"
    ) as response:
        html = await response.text("latin-1")
        data = await parse_category(html, name, db_cls)
        return data


async def fetch_all_categories(urls):
    async with aiohttp.ClientSession() as session:
        results = await asyncio.gather(
            *[fetch_category(session, name, db_cls) for name, db_cls in urls],
            return_exceptions=True,
        )
        return results


async def fetch_page_raw(session, game_id, typ, url):
    async with session.get(url, verify_ssl=False) as response:
        html = await response.text("latin-1")
    return game_id, typ, html


def parse_game_page(game_id, html_game, html_reviews):
    item = BeautifulSoup(html_game, features="lxml")
    reviews_soup = BeautifulSoup(html_reviews, features="lxml")

    data = defaultdict(lambda: defaultdict(dict))
    data["authors"] = {}
    data["versions"] = defaultdict(list)
    data["play_online"] = None
    data["likes"] = 0
    data["reviews"] = []

    data["title"] = item.find(class_="viewgamecontenttitle").text.strip()

    # Game

    container = item.find(class_="viewgamecontentauthor")
    links = container.find_all("a")
    if links:
        for link in links:
            try:
                data["authors"][link.text.lower().replace(" ", "_")] = int(
                    link.get("href").split("u=")[1]
                )
            except:
                continue
    else:
        try:
            author = (
                container.text.strip().lstrip("by").strip().lower().replace(" ", "_")
            )
            data["authors"][author] = GameAuthor.get(name=author).id
        except:
            return

    game_info = item.select(".viewgamesidecontainer > .viewgameanothercontainer")[0]

    for box in game_info.find_all(class_="viewgameinfo"):
        left = box.find(class_="viewgameitemleft").text
        right = box.find(class_="viewgameitemright")

        if left == "Engine":
            data["game_engine"] = right.text.lower().replace(" ", "_")
        elif left == "Rating":
            data["content_rating"] = right.text.lower().replace(" ", "_")
        elif left == "Language":
            data["language"] = right.text
        elif left == "Release Date":
            result = right.text
            try:
                data["release_date"] = arrow.get(
                    result, "|DD MMM YYYY|, HH:mm"
                ).datetime
            except:
                pass
            try:
                data["release_date"] = arrow.get(result, "MM/DD/YYYY").datetime
            except:
                pass
        elif left == "Last Update":
            result = right.text
            try:
                data["last_update"] = arrow.get(result, "|DD MMM YYYY|, HH:mm").datetime
            except:
                pass
            try:
                data["last_update"] = arrow.get(result, "MM/DD/YYYY").datetime
            except:
                pass
        elif left == "Version":
            data["version"] = right.text
        elif left == "Development":
            data["development_stage"] = right.text
        elif left == "Likes":
            data["likes"] = int(right.text)
        elif left == "Contest":
            result = right.text
            data["contest"] = None if result == "None" else result
        elif left == "Orig PC Gender":
            data["orig_pc_gender"] = right.text
        elif left == "Adult Themes":
            for link in right.find_all("a"):
                data["themes"]["adult"][link.text] = int(
                    link.get("href").split("adult=")[1]
                )
        elif left == "TF Themes":
            for link in right.find_all("a"):
                data["themes"]["transformation"][link.text] = int(
                    link.get("href").split("transformation=")[1]
                )
        elif left == "Multimedia":
            for link in right.find_all("a"):
                data["themes"]["multimedia"][link.text] = int(
                    link.get("href").split("multimedia=")[1]
                )
        elif left == "Discussion/Help":
            data["thread"] = right.find("a").get("href")

    downloads = item.find(id="downloads")

    for container in downloads:
        if container.name == "center":
            version = container.text.lstrip("Version:").strip()
        elif container.name == "div":
            link = {}
            link["delete"] = container.find(class_="dldeadlink").find("a")
            link["link"] = container.find(class_="dltext").find("a").get("href")
            try:
                link["note"] = container.find(class_="dlnotes").find("img").get("title")
            except:
                link["note"] = None
            link["report"] = urljoin(
                config.BASE_URL,
                container.find(class_="dlreportdeadlink").find("a").get("href"),
            )
            data["versions"][version or data["version"]].append(link)

    for i in range(1, 6):
        tab = item.find(id=f"tabs-{i}")
        if not tab:
            continue
        title = item.find("a", {"href": f"#tabs-{i}"}).text
        data[title.lower()] = {}
        data[title.lower()]["text"] = tab.text
        data[title.lower()]["html"] = str(tab)

    play_online = item.find(id="play")
    if play_online:
        data["play_online"] = play_online.find("form").get("action")

    # Reviews

    for i, review in enumerate(reversed(reviews_soup.find_all(class_="reviewcontent"))):
        lines = [line for line in review.text.split("\n") if line.strip()]
        if "Review by" not in lines[0]:
            continue
        author = lines[0].lstrip("Review by").strip()
        m = re.match(r"Version reviewed: (.+) on (.*)", lines[1])
        if not m:
            continue
        version, date = m.groups()
        try:
            date = arrow.get(date, "YYYY-MM-DD HH:mm:ss")
        except:
            date = arrow.get(date, "MM/DD/YYYY HH:mm:ss")
        text = "\n".join(lines[2:])
        if not text:
            continue
        data["reviews"].append(
            PReview(
                id=i,
                author=author,
                version=version,
                date=date.datetime,
                text=text,
            )
        )

    return PGame(id=game_id, **data)


async def crawl_tfgs():
    db.drop_all_tables(with_all_data=True)
    db.create_tables()

    with db_session:
        print("Fetching Categories")
        result = await fetch_all_categories(
            [
                ("engine", GameEngine),
                ("rating", ContentRating),
                ("adult", AdultTheme),
                ("transformation", TransformationTheme),
                ("multimedia", MultimediaTheme),
                ("author", GameAuthor),
            ]
        )

        print("Fetching list of games")
        payload = "module=search&search=1&likesmin=0&likesmax=0&development%5B%5D=11&development%5B%5D=12&development%5B%5D=18&development%5B%5D=41&development%5B%5D=46&development%5B%5D=47"

        headers = {"Content-Type": "application/x-www-form-urlencoded"}

        async with aiohttp.ClientSession() as session:
            async with session.post(
                "https://tfgames.site/index.php", data=payload, headers=headers
            ) as response:
                if not response.status == 200:
                    raise Exception("Not status code 200")
                html = await response.text("latin-1")

        soup = BeautifulSoup(html, features="lxml")

        table = soup.find("table")

        game_links = []
        for row in table.find_all("tr"):
            cols = row.find_all("td")

            if not cols:
                continue

            game_links.append(
                urljoin(config.BASE_URL, f'/{cols[0].find("a").get("href")}')
            )

        print("Fetching game info")

        all_links = []
        for url in sorted(game_links)[:100]:
            game_id = int(url.split("id=")[1])
            all_links.append((game_id, "game", url))
            all_links.append(
                (
                    game_id,
                    "reviews",
                    f"https://tfgames.site/modules/viewgame/viewreviews.php?id={game_id}",
                )
            )

        result = defaultdict(dict)
        async with aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=100)
        ) as session:
            tasks = [
                fetch_page_raw(session, game_id, typ, url)
                for game_id, typ, url in all_links
            ]
            for task in asyncio.as_completed(tasks):
                game_id, typ, html = await task
                result[game_id][typ] = html

        print("Parsing info")
        games = []
        for game_id, data in result.items():
            game = parse_game_page(game_id, data["game"], data["reviews"])
            games.append(game)

        print("Writing to database")
        for game in games:
            if not game:
                continue

            try:
                pgame_to_db_game(game)
            except Exception as e:
                print(game.id)
                raise e

        print("Done")


@app.post("/crawl", tags=["crawl"])
def trigger_crawl(background_tasks: BackgroundTasks):
    """
    Crawls TFGS and upserts new data into the database.
    """
    background_tasks.add_task(crawl_tfgs)
