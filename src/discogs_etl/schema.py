import pyarrow as pa

master_schema = pa.schema([
    ('id', pa.int64()),
    ('main_release', pa.int64()),
    ('artists', pa.list_(pa.struct([
        ('id', pa.int64()),
        ('name', pa.string()),
        ('anv', pa.string()),
        ('join', pa.string()),
        ('role', pa.string()),
        ('tracks', pa.string())
    ]))),
    ('genres', pa.list_(pa.string())),
    ('styles', pa.list_(pa.string())),
    ('year', pa.int32()),
    ('title', pa.string()),
    ('data_quality', pa.string()),
    ('images', pa.list_(pa.struct([
        ('height', pa.int32()),
        ('width', pa.int32()),
        ('type', pa.string()),
        ('uri', pa.string()),
        ('uri150', pa.string())
    ]))),
    ('videos', pa.list_(pa.struct([
        ('duration', pa.int32()),
        ('embed', pa.bool_()),
        ('src', pa.string()),
        ('title', pa.string()),
        ('description', pa.string())
    ])))
])

label_schema = pa.schema([
        ('id', pa.int64()),
        ('name', pa.string()),
        ('contactinfo', pa.string()),
        ('profile', pa.string()),
        ('data_quality', pa.string()),
        ('images', pa.list_(pa.struct([
            ('width', pa.int32()),
            ('height', pa.int32()),
            ('type', pa.string()),
            ('uri', pa.string()),
            ('uri150', pa.string())
        ]))),
        ('urls', pa.list_(pa.string())),
        ('sublabels', pa.list_(pa.string()))
    ])

release_schema = pa.schema([
        ('id', pa.int64()),
        ('status', pa.string()),
        ('title', pa.string()),
        ('country', pa.string()),
        ('released', pa.string()),
        ('notes', pa.string()),
        ('images', pa.list_(pa.struct([
            ('height', pa.int32()),
            ('width', pa.int32()),
            ('type', pa.string()),
            ('uri', pa.string()),
            ('uri150', pa.string())
        ]))),
        ('artists', pa.list_(pa.string())),
        ('labels', pa.list_(pa.struct([
            ('name', pa.string()),
            ('catno', pa.string())
        ]))),
        ('formats', pa.list_(pa.struct([
            ('name', pa.string()),
            ('qty', pa.string()),
            ('descriptions', pa.list_(pa.string()))
        ]))),
        ('genres', pa.list_(pa.string())),
        ('styles', pa.list_(pa.string()))
    ])

artist_schema = pa.schema([
        ('id', pa.int64()),
        ('name', pa.string()),
        ('realname', pa.string()),
        ('profile', pa.string()),
        ('data_quality', pa.string()),
        ('urls', pa.list_(pa.string())),
        ('namevariations', pa.list_(pa.string())),
        ('aliases', pa.list_(pa.string())),
        ('groups', pa.list_(pa.string())),
        ('members', pa.list_(pa.string())),
        ('images', pa.list_(pa.struct([
            ('height', pa.int32()),
            ('width', pa.int32()),
            ('type', pa.string()),
            ('uri', pa.string()),
            ('uri150', pa.string())
        ])))
    ])

SCHEMAS = {
    "master": master_schema,
    "label": label_schema,
    "release": release_schema,
    "artist": artist_schema,
}