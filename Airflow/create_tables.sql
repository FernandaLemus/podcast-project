CREATE TABLE public.podcast_agg_reviews (
	podcast_id varchar(256) NOT NULL,
	title varchar(256),
	avg_rating numeric(18,0),
	total_ratings numeric(18,0),
    CONSTRAINT podcast_pkey PRIMARY KEY (podcast_id)
);

CREATE TABLE public.categories_agg_reviews(
	category_id INT IDENTITY(1,1) NOT NULL,
	category varchar(256) NOT NULL,
	total_podcast int4,
	category_avg_rating numeric(18,0)
);

CREATE TABLE public.staging_category (
	podcast_id varchar(256),
	category varchar(256)
);

CREATE TABLE public.staging_podcast (
	podcast_id varchar(256),
	itunes_id bigint,
	slug varchar(256),
	itunes_url varchar(256),
	title varchar(256)
);

CREATE TABLE public.staging_reviews (
	podcast_id varchar(256),
	title varchar(max),
	content varchar(max),
	rating int4,
	created_at varchar(256)
);

CREATE TABLE public.podcast_dim (
	podcast_id varchar(max) NOT NULL,
	podcast_name varchar(max) NOT NULL,
	podcast_category varchar(max),
	itunes_url varchar(max)
);
