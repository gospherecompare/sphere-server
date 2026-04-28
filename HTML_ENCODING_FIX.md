# HTML Encoding Issue - Fix Documentation

## Problem

HTML content in blog articles was being stored and transmitted with encoded entities:

- `&lt;` instead of `<`
- `&gt;` instead of `>`
- `&quot;` instead of `"`
- `&#39;` instead of `'`

This caused HTML tags to display as plain text instead of being rendered as formatted content.

## Root Cause

The content was being double-encoded or stored with HTML entities in the database, preventing proper rendering in the frontend UI.

## Solution Implemented

### 1. Backend Utility (`server/utils/htmlDecoder.js`)

Created a utility module with two functions:

**`decodeHtmlEntities(text)`**

- Converts HTML entities back to their proper characters
- Handles: `&lt;`, `&gt;`, `&quot;`, `&#39;`, `&amp;`

**`ensureProperHtmlEncoding(content)`**

- Detects if content is double-encoded
- Automatically decodes it before sending via API
- Prevents encoding issues in JSON responses

### 2. Backend API Updates

Updated all blog endpoints to decode HTML content before returning:

**Public API:**

- `GET /api/public/blogs` - List view with proper HTML decoding
- `GET /api/public/blogs/:slug` - Detail view with proper HTML decoding

**Admin API:**

- `GET /api/admin/blogs/:id` - Get single blog with proper HTML decoding
- `POST /api/admin/blogs` - Save blog with proper HTML decoding
- `GET /api/admin/blogs` - List blogs with proper HTML decoding

**Helper Function:**

- `resolvePublicBlogRow()` - Automatically decodes content for resolved blogs

### 3. Frontend Usage

The frontend is already properly configured to render HTML using `dangerouslySetInnerHTML`:

```jsx
<div
  dangerouslySetInnerHTML={{ __html: articleHtml }}
  className="article-content"
/>
```

The backend now ensures that the content passed to this is properly decoded.

## Database Impact

No database changes needed - content remains stored as HTML text. The decoding happens at the API response layer only.

## Testing

To verify the fix:

1. **Create/Update a Blog**
   - Add HTML content with `<h3>`, `<p>`, `<table>` tags
   - Save the blog

2. **Check API Response**
   - Call GET `/api/public/blogs/:slug`
   - Verify tags show as `<h3>` not `&lt;h3&gt;`

3. **Verify Frontend Rendering**
   - Visit the news/blog page
   - Confirm proper formatting with headings, paragraphs, and tables

## Files Changed

- `server/utils/htmlDecoder.js` - NEW (Utility functions)
- `server/index.js` - UPDATED (5 endpoints + imports)

## Performance Notes

- Minimal overhead: decoding runs only at API response time
- Cached: No additional database queries
- Optional: Only decodes if double-encoding is detected
