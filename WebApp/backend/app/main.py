from fastapi import FastAPI, Depends, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from decimal import Decimal
import logging
from typing import Optional

from .db import get_db
from . import models, schemas

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Chinook Store API",
    description="A complete music store API based on the Chinook database",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Track endpoints
@app.get("/tracks", response_model=list[schemas.TrackOut])
def get_tracks(
    limit: int = Query(default=50, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    name: Optional[str] = Query(default=None),
    album_id: Optional[int] = Query(default=None),
    genre_id: Optional[int] = Query(default=None),
    min_price: Optional[Decimal] = Query(default=None, ge=0),
    max_price: Optional[Decimal] = Query(default=None, ge=0),
    db: Session = Depends(get_db)
):
    """Get tracks with optional filtering"""
    try:
        query = db.query(models.Track)
        
        if name:
            query = query.filter(models.Track.Name.ilike(f"%{name}%"))
        if album_id:
            query = query.filter(models.Track.AlbumId == album_id)
        if genre_id:
            query = query.filter(models.Track.GenreId == genre_id)
        if min_price:
            query = query.filter(models.Track.UnitPrice >= min_price)
        if max_price:
            query = query.filter(models.Track.UnitPrice <= max_price)
            
        tracks = query.limit(limit).offset(offset).all()
        logger.info(f"Retrieved {len(tracks)} tracks")
        return tracks
    except Exception as e:
        logger.error(f"Error retrieving tracks: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/tracks/{track_id}", response_model=schemas.TrackOut)
def get_track(track_id: int, db: Session = Depends(get_db)):
    """Get a specific track by ID"""
    try:
        track = db.query(models.Track).filter(models.Track.TrackId == track_id).first()
        if not track:
            raise HTTPException(status_code=404, detail="Track not found")
        return track
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving track {track_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


# Album endpoints
@app.get("/albums", response_model=list[schemas.AlbumOut])
def get_albums(limit: int = Query(default=50, ge=1, le=100), offset: int = Query(default=0, ge=0), db: Session = Depends(get_db)):
    """Get albums"""
    try:
        albums = db.query(models.Album).limit(limit).offset(offset).all()
        return albums
    except Exception as e:
        logger.error(f"Error retrieving albums: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/albums/{album_id}", response_model=schemas.AlbumOut)
def get_album(album_id: int, db: Session = Depends(get_db)):
    """Get a specific album by ID"""
    try:
        album = db.query(models.Album).filter(models.Album.AlbumId == album_id).first()
        if not album:
            raise HTTPException(status_code=404, detail="Album not found")
        return album
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving album {album_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


# Artist endpoints
@app.get("/artists", response_model=list[schemas.ArtistOut])
def get_artists(limit: int = Query(default=50, ge=1, le=100), offset: int = Query(default=0, ge=0), db: Session = Depends(get_db)):
    """Get artists"""
    try:
        artists = db.query(models.Artist).limit(limit).offset(offset).all()
        return artists
    except Exception as e:
        logger.error(f"Error retrieving artists: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/artists/{artist_id}", response_model=schemas.ArtistOut)
def get_artist(artist_id: int, db: Session = Depends(get_db)):
    """Get a specific artist by ID"""
    try:
        artist = db.query(models.Artist).filter(models.Artist.ArtistId == artist_id).first()
        if not artist:
            raise HTTPException(status_code=404, detail="Artist not found")
        return artist
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving artist {artist_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


# Genre endpoints
@app.get("/genres", response_model=list[schemas.GenreOut])
def get_genres(db: Session = Depends(get_db)):
    """Get all genres"""
    try:
        genres = db.query(models.Genre).all()
        return genres
    except Exception as e:
        logger.error(f"Error retrieving genres: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


# Customer endpoints
@app.post("/customers", response_model=schemas.CustomerResponse, status_code=status.HTTP_201_CREATED)
def create_customer(data: schemas.CustomerCreate, db: Session = Depends(get_db)):
    """Create a new customer"""
    try:
        customer = models.Customer(
            FirstName=data.FirstName,
            LastName=data.LastName,
            Email=data.Email
        )
        db.add(customer)
        db.commit()
        db.refresh(customer)
        logger.info(f"Created customer {customer.CustomerId}")
        return schemas.CustomerResponse(CustomerId=customer.CustomerId)
    except IntegrityError as e:
        db.rollback()
        logger.warning(f"Customer creation failed - integrity error: {str(e)}")
        if "unique constraint" in str(e).lower() or "duplicate" in str(e).lower():
            raise HTTPException(status_code=400, detail="Email already exists")
        raise HTTPException(status_code=400, detail="Invalid customer data")
    except Exception as e:
        db.rollback()
        logger.error(f"Error creating customer: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/customers/{customer_id}", response_model=schemas.CustomerOut)
def get_customer(customer_id: int, db: Session = Depends(get_db)):
    """Get a specific customer by ID"""
    try:
        customer = db.query(models.Customer).filter(models.Customer.CustomerId == customer_id).first()
        if not customer:
            raise HTTPException(status_code=404, detail="Customer not found")
        return customer
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving customer {customer_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


# Checkout endpoint
@app.post("/checkout", response_model=schemas.CheckoutResponse, status_code=status.HTTP_201_CREATED)
def checkout(data: schemas.CheckoutIn, db: Session = Depends(get_db)):
    """Process checkout and create an invoice"""
    try:
        # Validate customer exists
        customer = db.query(models.Customer).filter(models.Customer.CustomerId == data.CustomerId).first()
        if not customer:
            raise HTTPException(status_code=404, detail="Customer not found")

        # Validate all tracks exist and calculate total
        total = Decimal("0.00")
        track_data = []
        
        for item in data.Items:
            track = db.query(models.Track).filter(models.Track.TrackId == item.TrackId).first()
            if not track:
                raise HTTPException(status_code=400, detail=f"Track {item.TrackId} not found")
            
            # Verify price matches current track price
            if item.UnitPrice != track.UnitPrice:
                logger.warning(f"Price mismatch for track {item.TrackId}: {item.UnitPrice} vs {track.UnitPrice}")
                # Use current track price for security
                item.UnitPrice = track.UnitPrice
            
            track_data.append((track, item))
            total += item.UnitPrice * item.Quantity

        # Create invoice
        invoice = models.Invoice(
            CustomerId=data.CustomerId,
            BillingAddress=data.BillingAddress,
            BillingCity=data.BillingCity,
            BillingCountry=data.BillingCountry,
            Total=total
        )
        db.add(invoice)
        db.flush()  # Get invoice ID

        # Create invoice lines
        for track, item in track_data:
            line = models.InvoiceLine(
                InvoiceId=invoice.InvoiceId,
                TrackId=item.TrackId,
                UnitPrice=item.UnitPrice,
                Quantity=item.Quantity
            )
            db.add(line)

        db.commit()
        db.refresh(invoice)
        logger.info(f"Created invoice {invoice.InvoiceId} for customer {data.CustomerId}")
        
        return schemas.CheckoutResponse(
            InvoiceId=invoice.InvoiceId,
            Total=str(invoice.Total)
        )
        
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"Error processing checkout: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


# Invoice endpoints
@app.get("/invoices/{invoice_id}", response_model=schemas.InvoiceOut)
def get_invoice(invoice_id: int, db: Session = Depends(get_db)):
    """Get a specific invoice by ID"""
    try:
        invoice = db.query(models.Invoice).filter(models.Invoice.InvoiceId == invoice_id).first()
        if not invoice:
            raise HTTPException(status_code=404, detail="Invoice not found")
        return invoice
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving invoice {invoice_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


# Health check
@app.get("/health")
def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "Chinook Store API"}
