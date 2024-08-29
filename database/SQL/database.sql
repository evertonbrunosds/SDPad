CREATE TABLE profile (
    id_profile_pk uuid NOT NULL DEFAULT uuid_generate_v4() PRIMARY KEY,
    email character varying(262) NOT NULL UNIQUE,
    password character(60) NOT NULL CHECK(LENGTH(password) = 60),
    created_at timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE writer (
    id_writer_pk uuid NOT NULL DEFAULT uuid_generate_v4() PRIMARY KEY,
    id_profile_fk uuid NOT NULL,
    name character varying(32) NOT NULL UNIQUE CHECK (name::text ~ '^(?!_)(?!.*__)[a-z0-9_]+(?<!_)$'::text),
    label character varying(32) NOT NULL CHECK (label::text ~ '^(?! )(?!.*  )[a-zA-Z0-9À-ÿ ]+(?<! )$'::text),
    description character varying(1024),
    birthday date NOT NULL CHECK (EXTRACT(year FROM age(CURRENT_DATE::timestamp with time zone, birthday::timestamp with time zone)) >= 18::numeric)
);

CREATE TABLE pending_writer (
    id_pending_writer_pk uuid NOT NULL DEFAULT uuid_generate_v4() PRIMARY KEY,
    id_writer_fk uuid NOT NULL,
    activation_key char(192) NOT NULL CHECK(length(activation_key) = 192),
    created_at timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP
);
 
ALTER TABLE writer ADD CONSTRAINT writer_fkey
    FOREIGN KEY (id_profile_fk)
    REFERENCES profile (id_profile_pk);
 
ALTER TABLE pending_writer ADD CONSTRAINT pending_writer_fkey
    FOREIGN KEY (id_writer_fk)
    REFERENCES writer (id_writer_pk);
