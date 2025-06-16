// MongoDB CRUD operations, advanced queries, aggregation pipelines, and indexing

const { MongoClient } = require('mongodb');
const uri = 'mongodb://localhost:27017';
const dbName = 'plp_bookstore';
const collectionName = 'books';

// Helper function to connect to MongoDB
async function connectToMongoDB() {
  const client = new MongoClient(uri);
  await client.connect();
  return client.db(dbName).collection(collectionName);
}

// CRUD Operations

// 1. Create - Insert a new book
async function createBook(bookData) {
  const collection = await connectToMongoDB();
  const result = await collection.insertOne(bookData);
  console.log(`Inserted book with id: ${result.insertedId}`);
  return result;
}

// 2. Read - Find a book by title
async function readBook(title) {
  const collection = await connectToMongoDB();
  const book = await collection.findOne({ title });
  console.log('Found book:', book);
  return book;
}

// 3. Update - Update a book's price
async function updateBookPrice(title, newPrice) {
  const collection = await connectToMongoDB();
  const result = await collection.updateOne(
    { title },
    { $set: { price: newPrice } }
  );
  console.log(`Updated ${result.modifiedCount} book(s)`);
  return result;
}

// 4. Delete - Remove a book
async function deleteBook(title) {
  const collection = await connectToMongoDB();
  const result = await collection.deleteOne({ title });
  console.log(`Deleted ${result.deletedCount} book(s)`);
  return result;
}

// Advanced Queries

// 1. Find books by genre with projection
async function findBooksByGenre(genre) {
  const collection = await connectToMongoDB();
  const books = await collection.find(
    { genre },
    { projection: { title: 1, author: 1, price: 1, _id: 0 } }
  ).toArray();
  console.log(`Books in ${genre} genre:`, books);
  return books;
}

// 2. Find books published between years, sorted by publication year
async function findBooksBetweenYears(startYear, endYear) {
  const collection = await connectToMongoDB();
  const books = await collection.find({
    published_year: { $gte: startYear, $lte: endYear }
  }).sort({ published_year: 1 }).toArray();
  console.log(`Books published between ${startYear} and ${endYear}:`, books);
  return books;
}

// 3. Find books with price less than specified amount, in stock
async function findAffordableBooksInStock(maxPrice) {
  const collection = await connectToMongoDB();
  const books = await collection.find({
    price: { $lt: maxPrice },
    in_stock: true
  }).toArray();
  console.log(`Books under $${maxPrice} in stock:`, books);
  return books;
}

// Aggregation Pipelines

// 1. Average price by genre
async function getAveragePriceByGenre() {
  const collection = await connectToMongoDB();
  const result = await collection.aggregate([
    { $group: { _id: '$genre', averagePrice: { $avg: '$price' } } },
    { $sort: { averagePrice: -1 } }
  ]).toArray();
  console.log('Average price by genre:', result);
  return result;
}

// 2. Count of books by publication decade
async function getBooksByDecade() {
  const collection = await connectToMongoDB();
  const result = await collection.aggregate([
    {
      $project: {
        decade: {
          $subtract: [
            '$published_year',
            { $mod: ['$published_year', 10] }
          ]
        }
      }
    },
    { $group: { _id: '$decade', count: { $sum: 1 } } },
    { $sort: { _id: 1 } }
  ]).toArray();
  console.log('Books by decade:', result);
  return result;
}

// 3. Most expensive book in each genre
async function getMostExpensiveByGenre() {
  const collection = await connectToMongoDB();
  const result = await collection.aggregate([
    { $sort: { genre: 1, price: -1 } },
    {
      $group: {
        _id: '$genre',
        title: { $first: '$title' },
        author: { $first: '$author' },
        price: { $first: '$price' }
      }
    }
  ]).toArray();
  console.log('Most expensive books by genre:', result);
  return result;
}

// Indexing

// 1. Create index on title field
async function createTitleIndex() {
  const collection = await connectToMongoDB();
  const result = await collection.createIndex({ title: 1 });
  console.log('Created index on title:', result);
  return result;
}

// 2. Create compound index on genre and published_year
async function createGenreYearIndex() {
  const collection = await connectToMongoDB();
  const result = await collection.createIndex({ genre: 1, published_year: -1 });
  console.log('Created compound index on genre and published_year:', result);
  return result;
}

// 3. Get all indexes
async function listIndexes() {
  const collection = await connectToMongoDB();
  const indexes = await collection.indexes();
  console.log('Current indexes:', indexes);
  return indexes;
}

// usage
(async () => {
  // CRUD operations
  await createBook({
    title: 'The Silent Patient',
    author: 'Alex Michaelides',
    genre: 'Thriller',
    published_year: 2019,
    price: 13.99,
    in_stock: true,
    pages: 325,
    publisher: 'Celadon Books'
  });
  
  await readBook('1984');
  await updateBookPrice('The Hobbit', 15.99);
  await deleteBook('Moby Dick');

  // Advanced queries
  await findBooksByGenre('Fiction');
  await findBooksBetweenYears(1900, 1950);
  await findAffordableBooksInStock(10);

  // Aggregation pipelines
  await getAveragePriceByGenre();
  await getBooksByDecade();
  await getMostExpensiveByGenre();

  // Indexing
  await createTitleIndex();
  await createGenreYearIndex();
  await listIndexes();
})();



// == TERMINAL LOG ==

/*

>_ node queries.js

Inserted book with id: 684fdb327e4b37b629b991a1
Found book: {
  _id: new ObjectId('684fd3bbe20c2ff5e98dc913'),
  title: '1984',
  author: 'George Orwell',
  genre: 'Dystopian',
  published_year: 1949,
  price: 10.99,
  in_stock: true,
  pages: 328,
  publisher: 'Secker & Warburg'
}
Updated 1 book(s)
Deleted 1 book(s)
Books in Fiction genre: [
  {
    title: 'To Kill a Mockingbird',
    author: 'Harper Lee',
    price: 12.99
  },
  {
    title: 'The Great Gatsby',
    author: 'F. Scott Fitzgerald',
    price: 9.99
  },
  {
    title: 'The Catcher in the Rye',
    author: 'J.D. Salinger',
    price: 8.99
  },
  { title: 'The Alchemist', author: 'Paulo Coelho', price: 10.99 }
]
Books published between 1900 and 1950: [
  {
    _id: new ObjectId('684fd3bbe20c2ff5e98dc914'),
    title: 'The Great Gatsby',
    author: 'F. Scott Fitzgerald',
    genre: 'Fiction',
    published_year: 1925,
    price: 9.99,
    in_stock: true,
    pages: 180,
    publisher: "Charles Scribner's Sons"
  },
  {
    _id: new ObjectId('684fd3bbe20c2ff5e98dc915'),
    title: 'Brave New World',
    author: 'Aldous Huxley',
    genre: 'Dystopian',
    published_year: 1932,
    price: 11.5,
    in_stock: false,
    pages: 311,
    publisher: 'Chatto & Windus'
  },
  {
    _id: new ObjectId('684fd3bbe20c2ff5e98dc916'),
    title: 'The Hobbit',
    author: 'J.R.R. Tolkien',
    genre: 'Fantasy',
    published_year: 1937,
    price: 15.99,
    in_stock: true,
    pages: 310,
    publisher: 'George Allen & Unwin'
  },
  {
    _id: new ObjectId('684fd3bbe20c2ff5e98dc91a'),
    title: 'Animal Farm',
    author: 'George Orwell',
    genre: 'Political Satire',
    published_year: 1945,
    price: 8.5,
    in_stock: false,
    pages: 112,
    publisher: 'Secker & Warburg'
  },
  {
    _id: new ObjectId('684fd3bbe20c2ff5e98dc913'),
    title: '1984',
    author: 'George Orwell',
    genre: 'Dystopian',
    published_year: 1949,
    price: 10.99,
    in_stock: true,
    pages: 328,
    publisher: 'Secker & Warburg'
  }
]
Books under $10 in stock: [
  {
    _id: new ObjectId('684fd3bbe20c2ff5e98dc914'),
    title: 'The Great Gatsby',
    author: 'F. Scott Fitzgerald',
    genre: 'Fiction',
    published_year: 1925,
    price: 9.99,
    in_stock: true,
    pages: 180,
    publisher: "Charles Scribner's Sons"
  },
  {
    _id: new ObjectId('684fd3bbe20c2ff5e98dc917'),
    title: 'The Catcher in the Rye',
    author: 'J.D. Salinger',
    genre: 'Fiction',
    published_year: 1951,
    price: 8.99,
    in_stock: true,
    pages: 224,
    publisher: 'Little, Brown and Company'
  },
  {
    _id: new ObjectId('684fd3bbe20c2ff5e98dc918'),
    title: 'Pride and Prejudice',
    author: 'Jane Austen',
    genre: 'Romance',
    published_year: 1813,
    price: 7.99,
    in_stock: true,
    pages: 432,
    publisher: 'T. Egerton, Whitehall'
  },
  {
    _id: new ObjectId('684fd3bbe20c2ff5e98dc91d'),
    title: 'Wuthering Heights',
    author: 'Emily Brontë',
    genre: 'Gothic Fiction',
    published_year: 1847,
    price: 9.99,
    in_stock: true,
    pages: 342,
    publisher: 'Thomas Cautley Newby'
  }
]
Average price by genre: [
  { _id: 'Fantasy', averagePrice: 17.99 },
  { _id: 'Thriller', averagePrice: 13.99 },
  { _id: 'Dystopian', averagePrice: 11.245000000000001 },
  { _id: 'Fiction', averagePrice: 10.74 },
  { _id: 'Gothic Fiction', averagePrice: 9.99 },
  { _id: 'Political Satire', averagePrice: 8.5 },
  { _id: 'Romance', averagePrice: 7.99 }
]
Books by decade: [
  { _id: 1810, count: 1 },
  { _id: 1840, count: 1 },
  { _id: 1920, count: 1 },
  { _id: 1930, count: 2 },
  { _id: 1940, count: 2 },
  { _id: 1950, count: 2 },
  { _id: 1960, count: 1 },
  { _id: 1980, count: 1 },
  { _id: 2010, count: 1 }
]
Most expensive books by genre: [
  {
    _id: 'Thriller',
    title: 'The Silent Patient',
    author: 'Alex Michaelides',
    price: 13.99
  },
  {
    _id: 'Political Satire',
    title: 'Animal Farm',
    author: 'George Orwell',
    price: 8.5
  },
  {
    _id: 'Romance',
    title: 'Pride and Prejudice',
    author: 'Jane Austen',
    price: 7.99
  },
  {
    _id: 'Fantasy',
    title: 'The Lord of the Rings',
    author: 'J.R.R. Tolkien',
    price: 19.99
  },
  {
    _id: 'Dystopian',
    title: 'Brave New World',
    author: 'Aldous Huxley',
    price: 11.5
  },
  {
    _id: 'Fiction',
    title: 'To Kill a Mockingbird',
    author: 'Harper Lee',
    price: 12.99
  },
  {
    _id: 'Gothic Fiction',
    title: 'Wuthering Heights',
    author: 'Emily Brontë',
    price: 9.99
  }
]
Created index on title: title_1
Created compound index on genre and published_year: genre_1_published_year_-1
Current indexes: [
  { v: 2, key: { _id: 1 }, name: '_id_' },
  { v: 2, key: { title: 1 }, name: 'title_1' },
  {
    v: 2,
    key: { genre: 1, published_year: -1 },
    name: 'genre_1_published_year_-1'
  }
]

*/