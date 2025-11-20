from sqlalchemy import Column, Integer, String, Numeric, DateTime, ForeignKey, Index
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .db import Base


class Artist(Base):
    __tablename__ = "Artist"
    ArtistId = Column(Integer, primary_key=True, autoincrement=True)
    Name = Column(String(120), nullable=False)
    
    albums = relationship("Album", back_populates="artist")


class Album(Base):
    __tablename__ = "Album"
    AlbumId = Column(Integer, primary_key=True, autoincrement=True)
    Title = Column(String(160), nullable=False)
    ArtistId = Column(Integer, ForeignKey("Artist.ArtistId"), nullable=False)
    
    artist = relationship("Artist", back_populates="albums")
    tracks = relationship("Track", back_populates="album")


class Genre(Base):
    __tablename__ = "Genre"
    GenreId = Column(Integer, primary_key=True, autoincrement=True)
    Name = Column(String(120), nullable=False)
    
    tracks = relationship("Track", back_populates="genre")


class MediaType(Base):
    __tablename__ = "MediaType"
    MediaTypeId = Column(Integer, primary_key=True, autoincrement=True)
    Name = Column(String(120), nullable=False)
    
    tracks = relationship("Track", back_populates="media_type")


class Track(Base):
    __tablename__ = "Track"
    TrackId = Column(Integer, primary_key=True, autoincrement=True)
    Name = Column(String(200), nullable=False, index=True)
    AlbumId = Column(Integer, ForeignKey("Album.AlbumId"))
    MediaTypeId = Column(Integer, ForeignKey("MediaType.MediaTypeId"), nullable=False)
    GenreId = Column(Integer, ForeignKey("Genre.GenreId"))
    Composer = Column(String(220))
    Milliseconds = Column(Integer, nullable=False)
    Bytes = Column(Integer)
    UnitPrice = Column(Numeric(10, 2), nullable=False, index=True)
    
    album = relationship("Album", back_populates="tracks")
    genre = relationship("Genre", back_populates="tracks")
    media_type = relationship("MediaType", back_populates="tracks")
    invoice_lines = relationship("InvoiceLine", back_populates="track")

    __table_args__ = (
        Index('ix_track_album_genre', 'AlbumId', 'GenreId'),
        Index('ix_track_price_range', 'UnitPrice'),
    )


class Employee(Base):
    __tablename__ = "Employee"
    EmployeeId = Column(Integer, primary_key=True, autoincrement=True)
    LastName = Column(String(20), nullable=False)
    FirstName = Column(String(20), nullable=False)
    Title = Column(String(30))
    ReportsTo = Column(Integer, ForeignKey("Employee.EmployeeId"))
    BirthDate = Column(DateTime)
    HireDate = Column(DateTime)
    Address = Column(String(70))
    City = Column(String(40))
    State = Column(String(40))
    Country = Column(String(40))
    PostalCode = Column(String(10))
    Phone = Column(String(24))
    Fax = Column(String(24))
    Email = Column(String(60))
    
    manager = relationship("Employee", remote_side=[EmployeeId], backref="subordinates")
    customers = relationship("Customer", back_populates="support_rep")


class Customer(Base):
    __tablename__ = "Customer"
    CustomerId = Column(Integer, primary_key=True, autoincrement=True)
    FirstName = Column(String(40), nullable=False)
    LastName = Column(String(20), nullable=False)
    Company = Column(String(80))
    Address = Column(String(70))
    City = Column(String(40))
    State = Column(String(40))
    Country = Column(String(40))
    PostalCode = Column(String(10))
    Phone = Column(String(24))
    Fax = Column(String(24))
    Email = Column(String(60), nullable=False, unique=True, index=True)
    SupportRepId = Column(Integer, ForeignKey("Employee.EmployeeId"))
    
    invoices = relationship("Invoice", back_populates="customer")
    support_rep = relationship("Employee", back_populates="customers")


class Invoice(Base):
    __tablename__ = "Invoice"
    InvoiceId = Column(Integer, primary_key=True, autoincrement=True)
    CustomerId = Column(Integer, ForeignKey("Customer.CustomerId"), nullable=False)
    InvoiceDate = Column(DateTime, nullable=False, server_default=func.now(), index=True)
    BillingAddress = Column(String(70))
    BillingCity = Column(String(40))
    BillingState = Column(String(40))
    BillingCountry = Column(String(40))
    BillingPostalCode = Column(String(10))
    Total = Column(Numeric(10, 2), nullable=False)

    customer = relationship("Customer", back_populates="invoices")
    lines = relationship("InvoiceLine", back_populates="invoice", cascade="all, delete-orphan")

    __table_args__ = (
        Index('ix_invoice_customer_date', 'CustomerId', 'InvoiceDate'),
    )


class InvoiceLine(Base):
    __tablename__ = "InvoiceLine"
    InvoiceLineId = Column(Integer, primary_key=True, autoincrement=True)
    InvoiceId = Column(Integer, ForeignKey("Invoice.InvoiceId"), nullable=False)
    TrackId = Column(Integer, ForeignKey("Track.TrackId"), nullable=False)
    UnitPrice = Column(Numeric(10, 2), nullable=False)
    Quantity = Column(Integer, nullable=False)

    invoice = relationship("Invoice", back_populates="lines")
    track = relationship("Track", back_populates="invoice_lines")

    __table_args__ = (
        Index('ix_invoiceline_invoice_track', 'InvoiceId', 'TrackId'),
    )
